import httpx
from typing import List, Optional
from bs4 import BeautifulSoup
from pydantic import ValidationError

from ..schemas import BusService
import asyncio
import logging
import re
from ..config import OLLAMA_MODEL, OLLAMA_CONCURRENCY_LIMIT, TNSTC_DETAILS_URL, OLLAMA_BASE_URL
from tenacity import wait_exponential, stop_after_attempt, Retrying

import ollama
import json

from utils.clean_html import minify_html
from .prompt_builder import PromptGenerator

log = logging.getLogger(__name__)

class OllamaParser:
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
        You will be given two HTML fragments.
        1. MAIN_LIST_HTML: Contains the primary data for a single bus.
        2. DETAIL_TABLE_HTML: Contains supplementary data for the same bus.
        
        TASK:
        Extract every available field defined in the JSON_SCHEMA from these HTML fragments and merge data from both sources.

        ---
        MAIN_LIST_HTML
        {main_list_html}
        ---
        DETAIL_TABLE_HTML
        {detail_table_html}
        ---

        TASK:
        Extract all fields for a single JSON object. Follow these rules STRICTLY.

        **Data Location Rules (CRITICAL):**
        
        1.  **FROM MAIN_LIST_HTML (Primary Source):**
            * `operator` (e.g., "SALEM")
            * `bus_type` (e.g., "AC 3X2")
            * `departure_time` (e.g., "00:05")
            * `arrival_time` (e.g., "06:15")
            * `duration` (e.g., "6.10Hrs") Use the value ending in "Hrs" (e.g., "6.10Hrs" becomes "6.10"). return a normalized float-string in hours with 2 decimals. (6h10m -> "6.17")
        6. price and seats: prefer MAIN_LIST_HTML, use details list as fallback if not found.
            * `price_in_rs` (e.g., 195)
            * `seats_available` (e.g., 43)
            * `via_route`: Look in `MAIN_LIST_HTML` for text starting with "Via-". 
                (e.g., "Via-HOSUR"). Extract the place(s) as a JSON list. 
                Example: "Via-HOSUR" MUST become `["HOSUR"]`.
                Example: "Via-KARUR , DINDIGUL" MUST become `["KARUR", "DINDIGUL"]`.
                If not found, return `null`.

        2.  **FROM MAIN_LIST_HTML (Special Tags):**
            * `trip_code`: This is the long code inside the `<a>` tag.
            Trip code pattern hint: look for the longest contiguous alphanumeric uppercase token of length >=8 (e.g., 0005SALMADMM01L).
            Find the <a> tag. The trip_code is the text inside it. extract the text inside MAIN_LIST_HTML <b><a>...</a></b> (trim whitespace). If not found there, check DETAIL_TABLE_HTML.
                (e.g., from `<a> 0005SALMADMM01L</a>`, the trip_code is "0005SALMADMM01L").
                (Example: `<a> 0005SALMADMM01L</a>` -> "0005SALMADMM01L")
                (Example: `<a> 0030SALBANDD02A</a>` -> "0030SALBANDD02A")
                THIS IS *NOT* THE DEPARTURE TIME.
            * `route_code`: This is the short code after the " / " separator.
                This is the value usually (not everytime though) immediately after the " / " separator. 
                Often follows the trip code or appears near it; check MAIN_LIST_HTML first.
                (e.g., from `...</a></b> / 104N1`, the route_code is "104N1").
                (Example: `...</a></b> / 104N1` -> "104N1")
                (Example: `...</a></b> / 100J` -> "100J")
            * trip_code vs route_code: They are different fields. Do not confuse them. trip_code is the long one (0005SALMADMM01L), route_code is the short one (104N1).
                
            * **`total_kms`**: Look in `DETAIL_TABLE_HTML` for the label "Total Kms" or or something similar.
                The label might have an asterisk: "Total Kms * :". 
                The value is the number immediately following it (e.g., "208.00").
                If not found, you MUST return "NA".
            * `child_fare`: Look for a child fare.

        Failure Handling:
        * If `trip_code` or `route_code` are not in the `MAIN_LIST_HTML`, you *must* return "NA". DO NOT GUESS.
        * If `total_kms` is not in the `DETAIL_TABLE_HTML`, you *must* return "NA".
        * If `via_route` is not present, return `null`.
        * If a value is not found, return "NA".

        Return:
        → A single JSON object that conforms exactly to the JSON_SCHEMA provided in the system prompt.
        → Do not include any extra text, comments, or markdown.
        → If a value is not found, return "NA" for that field (or `null` for `via_route`).
        → Output strictly raw JSON.
        """
        
        messages = [
            {'role': 'system', 'content': self.system_prompt},
            {'role': 'user', 'content': user_prompt}
        ]
        
        retry_config = Retrying(
            wait=wait_exponential(multiplier=1, min=2, max=30),
            stop=stop_after_attempt(3),
            reraise=True
        )

        json_content = "" 
        for attempt in retry_config:
            with attempt:
                log.info(f"LLM_Parser Bus {bus_index} (Attempt {attempt.retry_state.attempt_number}): Sending HTML (Main: {len(main_list_html)} chars, Detail: {len(detail_table_html)} chars) to Ollama for JSON extraction.") 

                try:
                    response = await self.client.chat(
                        model=self.model,
                        messages=messages,
                        
                        format=self.json_schema,
                        options={
                            'temperature': 0.0
                        }
                    )

                    json_content = response['message']['content']
                    
                    service = BusService.model_validate_json(json_content)

                    log.info(f"LLM_Parser Bus {bus_index} SUCCESS: Extracted details for '{service.operator}' (Price: {service.price_in_rs}, Trip: {service.trip_code}).") 
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

    async def _call_load_trip_details(self, client: httpx.AsyncClient, onclick_attr: str, bus_index: int) -> str:
        """Extracts arguments and calls the LoadTripDetails endpoint."""
        args = re.findall(r"'([^']*)'", str(onclick_attr))
        if len(args) < 6:
            log.error(f"Failed to parse onclick_attr: {onclick_attr}")
            return ""

        data = {
            "ServiceID": args[0], "TripCode": args[1], "StartPlaceID": args[2],
            "EndPlaceID": args[3], "JourneyDate": args[4], "ClassID": args[5],
        }

        try:
            response = await client.post(TNSTC_DETAILS_URL, data=data)
            response.raise_for_status()
            return response.text
        except httpx.RequestError as e:
            log.error(f"Network error calling loadTripDetails for bus {bus_index}: {e}")
            return ""

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
        
        log.info(f"OllamaParser: Successfully parsed {len(bus_services)} / {len(bus_divs)} bus services.")
        
        return bus_services