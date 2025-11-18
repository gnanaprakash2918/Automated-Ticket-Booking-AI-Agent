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
        You are an expert parsing engine. You will receive two HTML fragments: a "Main List" item and a "Detail Table" popup.
        
        TASK:
        Extract specific fields defined in the JSON_SCHEMA.
        Merge data from both sources based on the "Source of Truth" hierarchy below.
        
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

        ### SOURCE OF TRUTH HIERARCHY (CRITICAL RULES)

        1. **STATIC DATA (Codes, Distance, Corp):**
           - **Source of Truth:** DETAIL_TABLE_HTML
           - **Fallback:** MAIN_LIST_HTML
           - Fields: `trip_code` (Service Code), `route_code` (Route No), `total_kms`, `operator` (Corporation).
           - *Note on Trip Code:* If the Detail Table is missing, check the MAIN_LIST <a> tag. If the text inside <a> is truncated (ends in '...'), check the `onclick` attribute arguments.

        2. **DYNAMIC DATA (Price, Seats, Time, Route):**
           - **Source of Truth:** MAIN_LIST_HTML
           - **Fallback:** DETAIL_TABLE_HTML
           - Fields: `price_in_rs`, `seats_available`, `departure_time`, `arrival_time`, `duration`, `via_route`, `bus_type`.
           - *Note on Price:* Main list price is the booking price. Detail table might show base fare. Use Main List.

        ### FIELD SPECIFIC EXTRACTION LOGIC

        * `via_route`: Look for text starting with "Via-" in MAIN_LIST. Split by comma. 
            - Example: "Via-KARUR , DINDIGUL" -> ["KARUR", "DINDIGUL"]
            - Example: "Via-HOSUR" -> ["HOSUR"]
            - If not found, return `null` (not "NA").
        
        * `child_fare`: Strictly from DETAIL_TABLE_HTML "Child Fare" column.
        
        * `duration`: Extract the numeric value string. "6.10Hrs" -> "6.10". "5:30" -> "5.30".
        
        * `price_in_rs`: Extract integers only. Remove "Rs" or symbols.

        ### FAILURE HANDLING
        * If a value is missing in Primary AND Fallback sources, return "NA".
        * Exceptions: `via_route` returns `null`, `price_in_rs` returns `0` if missing.

        ### OUTPUT FORMAT
        Output strictly raw JSON. No markdown, no conversational text.
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