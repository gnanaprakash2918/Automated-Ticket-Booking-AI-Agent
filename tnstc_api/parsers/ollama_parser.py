import httpx
from typing import List, Optional
from bs4 import BeautifulSoup
from pydantic import ValidationError

from ..schemas import BusService
import asyncio
import logging
import re
from ..config import OLLAMA_MODEL, OLLAMA_CONCURRENCY_LIMIT, OLLAMA_BASE_URL
from tenacity import wait_exponential, stop_after_attempt, Retrying

import ollama
import json

from utils.helpers import minify_html, calculate_message_size
from .prompt_builder import PromptGenerator
from .base import AbstractBusParser 

from utils.logging_setup import setup_logging

setup_logging()
log = logging.getLogger(__name__)

class OllamaParser(AbstractBusParser):
    """
    Implements the BusParser interface using a local LLM (via the native 'ollama' client)
    to parse HTML content chunk by chunk using JSON mode.
    """

    def __init__(self):
        
        try:
            self.client = ollama.AsyncClient(host=OLLAMA_BASE_URL)
            self.model = OLLAMA_MODEL
            self.prompt_gen = PromptGenerator()
            
            self.json_schema = BusService.model_json_schema()

            self.system_prompt = self.prompt_gen.build_system_prompt(BusService)
            
            self.few_shot_examples = self.prompt_gen._build_few_shot_examples()

            self.total_chars_sent = 0
            self.total_requests = 0

            log.info(f"OllamaParser initialized with native client. Model: {self.model}. Base URL: {OLLAMA_BASE_URL}")
            
        except ImportError:
            log.error("Ollama library not found. Please install 'ollama'")
            raise
        except Exception as e:
            log.error(f"Failed to initialize Ollama client: {e}")
            raise

    async def _parse_chunk_with_ollama(
        self,
        main_list_html: str,
        detail_table_html: str,
        bus_index: int
    ) -> Optional[BusService]:
        """
        Sends a single HTML chunk to the Ollama API for parsing and validation
        using the native 'ollama' client's JSON mode. This method is retryable via tenacity.
        """

        user_prompt = f"""
        You will be given two HTML fragments and examples of how to parse them.
        
        TASK:
        Extract every available field defined in the JSON_SCHEMA from the new HTML fragments provided at the end.
        Merge data from both sources.
        
        {self.few_shot_examples}

        ---
        NEW TASK
        ---
        MAIN_LIST_HTML
        {main_list_html}
        ---
        DETAIL_TABLE_HTML
        {minify_html(detail_table_html)}
        ---

        TASK:
        Extract all fields for a single JSON object based on the new data above.
        Follow the rules STRICTLY.

        **Data Location Rules (CRITICAL):**
        
        1.  **FROM MAIN_LIST_HTML (Primary Source):**
            * `operator` (e.g., "SALEM")
            * `bus_type` (e.g., "AC 3X2")
            * `departure_time` (e.g., "00:05")
            * `arrival_time` (e.g., "06:15")
            * `duration`: Extract the text value (e.g., "6.10Hrs" becomes "6.10").
            * `price_in_rs`: (e.g., 195)
            * `seats_available`: (e.g., 43)
            * `via_route`: Look for "Via-". Example: "Via-HOSUR" MUST become `["HOSUR"]`. If not found, return `null`.
            Example: "Via-KARUR , DINDIGUL" MUST become `["KARUR", "DINDIGUL"]`.
            If not found, return `null`.

        2.  **FROM MAIN_LIST_HTML (Special Tags):**
            * `trip_code`: Extract text inside the <a> tag (e.g., "0005SALMADMM01L"). 
            Trip code pattern hint: look for the longest contiguous alphanumeric uppercase token of length >=8 (e.g., 0005SALMADMM01L). **Look in MAIN_LIST_HTML first, use DETAIL_TABLE_HTML as a fallback.**
            * `route_code`: Extract the short code after the " / " separator (e.g., "104N1"). **Look in MAIN_LIST_HTML first, use DETAIL_TABLE_HTML as a fallback.**
            * trip_code vs route_code: They are different fields. Do not confuse them. trip_code is the long one (0005SALMADMM01L), route_code is the short one (104N1).

        3.  **FROM DETAIL_TABLE_HTML (Secondary Source):**
            * `total_kms`: Look for the label "Total Kms" and extract its value (e.g., "208.00"). The numeric value might be in the next strong tag or somewhere nearby.
            * `child_fare`: Look for a child fare.

        Failure Handling:
        * If any value is not found in its specified location, return "NA" (or `null` for `via_route`).
        * DO NOT GUESS.

        Return:
        -> A single JSON object that conforms exactly to the JSON_SCHEMA provided in the system prompt.
        -> Do not include any extra text, comments, or markdown.
        -> Output strictly raw JSON.
        """
        
        messages = [
            {'role': 'system', 'content': self.system_prompt},
            {'role': 'user', 'content': user_prompt}
        ]

        message_size = calculate_message_size(messages)
        system_size = len(self.system_prompt)
        user_size = len(user_prompt)
        
        retry_config = Retrying(
            wait=wait_exponential(multiplier=1, min=2, max=30),
            stop=stop_after_attempt(3),
            reraise=True
        )

        json_content = "" 
        for attempt in retry_config:
            with attempt:
                log.info( f"LLM_Parser Bus {bus_index} (Attempt {attempt.retry_state.attempt_number}): "
                    f"Sending {message_size} total chars to Ollama. "
                    f"Breakdown: System={system_size}, User={user_size}, "
                    f"HTML Input (Main={len(main_list_html)}, Detail={len(detail_table_html)}) "
                    f"-> JSON extraction."
                )

                self.total_chars_sent += message_size
                self.total_requests += 1

                try:
                    response = await self.client.chat(
                        model=self.model,
                        messages=messages,                        
                        format='json',
                        options={
                            'temperature': 0.0
                        }
                    )

                    json_content = response['message']['content']
                    
                    service = BusService.model_validate_json(json_content)

                    log.info( f"LLM_Parser Bus {bus_index} SUCCESS: Extracted '{service.operator}' "
                        f"(Price: {service.price_in_rs}, Trip: {service.trip_code}). "
                        f"Cumulative: {self.total_requests} requests, {self.total_chars_sent} chars sent."
                    )

                    return service
                
                except json.JSONDecodeError as e:
                    log.error(f"LLM_Parser Bus {bus_index}: Failed to decode JSON from LLM. Content: '{json_content[:150]}...'. Error: {e}", exc_info=True)
                    raise
                except ValidationError as e:
                    log.error(f"LLM_Parser Bus {bus_index}: Pydantic validation failed. Input: '{json_content[:150]}...'. Error: {e}", exc_info=True) 
                    raise
                except Exception as e:
                    log.error(f"OLLAMA_LOAD_TIMEOUT may be too low. Error during Ollama chat invocation: {e}", exc_info=True)
                    raise


    async def _wrapper_parse_chunk(
            self, 
            semaphore: asyncio.Semaphore, 
            main_list_html: str, 
            detail_table_html: str,
            idx: int
        ) -> Optional[BusService]:
            """
            A wrapper that acquires the semaphore before calling the
            parsing function.
            """
            log.debug(f"OllamaParser: [SEMAPHORE WAITING] for bus {idx}...")
            async with semaphore:
                log.info(f"OllamaParser: [SEMAPHORE ACQUIRED] Bus {idx}. Remaining slots: {semaphore._value}")
                try:
                    return await self._parse_chunk_with_ollama(
                        main_list_html, 
                        detail_table_html, 
                        idx
                    )
                finally:
                    log.debug(f"OllamaParser: [SEMAPHORE RELEASED] Finished chunk {idx}.")

    async def parse(
        self, 
        client: httpx.AsyncClient, 
        html_content: str,
        limit: Optional[int] = None
    ) -> List[BusService]:
        """
        Parses the main HTML by finding each bus, triggering its detail
        sub-request, and then parsing each bus individually using Ollama.
        """
        
        log.info(f"Using OllamaParser with model {OLLAMA_MODEL} (Native client strategy)...")
        semaphore = asyncio.Semaphore(OLLAMA_CONCURRENCY_LIMIT)
        log.info(f"Ollama concurrency limited to {OLLAMA_CONCURRENCY_LIMIT} simultaneous requests.")

        soup = BeautifulSoup(html_content, 'lxml')
        bus_divs = soup.find_all('div', class_ = 'bus-list')
        
        if not bus_divs:
            log.warning("OllamaParser: No 'div.bus-list' elements found in HTML.")
            return []
        
        if limit is not None:
            log.info(f"OllamaParser: Applying limit of {limit} buses.")
            bus_divs = bus_divs[:limit]

        # 1. Create tasks to fetch detailed HTML for all buses in parallel
        detail_tasks = []
        for idx, bus_div in enumerate(bus_divs):
            a_tag = bus_div.find("a", attrs={"data-target": "#TripcodePopUp", "onclick": True})
            onclick_attr = a_tag.get("onclick", "") if a_tag else ""

            if onclick_attr:
                detail_tasks.append(self._call_load_trip_details(client, str(onclick_attr), idx))
            else:
                future = asyncio.Future()
                future.set_result("")
                detail_tasks.append(future)
                log.warning(f"OllamaParser Bus {idx}: No 'onclick' attribute found. Cannot fetch details.")
        
        log.info(f"OllamaParser: Awaiting concurrent detail fetch for {len(detail_tasks)} buses...")
        all_details_html = await asyncio.gather(*detail_tasks, return_exceptions=True)

        # 2. Create tasks to parse each bus using the two HTML sources
        tasks = []
        for idx, bus_div in enumerate(bus_divs):
            main_list_html = re.sub(r"[\r\n]+", "", str(bus_div))
            detail_table_html = re.sub(r"[\r\n]+", "", str(all_details_html[idx]))

            main_list_html = minify_html(main_list_html)
            detail_table_html = minify_html(detail_table_html)
            tasks.append(
                self._wrapper_parse_chunk(
                    semaphore, 
                    main_list_html, 
                    detail_table_html, 
                    idx
                )
            )
        
        log.info(f"OllamaParser: Awaiting concurrent LLM parsing for {len(tasks)} buses...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        bus_services: List[BusService] = []
        for idx, res in enumerate(results):
            if isinstance(res, BusService):
                bus_services.append(res)
            elif isinstance(res, Exception):
                log.error(f"OllamaParser: Bus {idx}: Failed final parsing attempt after retries. Error: {res}")
        
        avg_chars = self.total_chars_sent / max(self.total_requests, 1)

        log.info(
            f"OllamaParser: Successfully parsed {len(bus_services)} / {len(bus_divs)} bus services. "
            f"Summary: {self.total_requests} requests, {self.total_chars_sent} total chars sent, "
            f"avg {avg_chars:.0f} chars/request."
        )

        
        return bus_services