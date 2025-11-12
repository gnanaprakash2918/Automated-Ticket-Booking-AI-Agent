import httpx
from typing import List, Optional
from bs4 import BeautifulSoup
from pydantic import ValidationError
from ..schemas import BusService
import json
import asyncio
import logging
import re
from ..config import OLLAMA_API_URL, OLLAMA_MODEL, OLLAMA_CONCURRENCY_LIMIT, TNSTC_DETAILS_URL
from tenacity import retry, wait_exponential, stop_after_attempt

log = logging.getLogger(__name__)

class OllamaParser:
    """
    Implements the BusParser interface using a local LLM (via Ollama)
    to parse HTML content chunk by chunk.
    """

    def __init__(self):
        self.bus_schema = json.dumps(BusService.model_json_schema(), indent=2)
        
        self.system_prompt = f"""
        You are an expert HTML parsing assistant. Your task is to extract data from
        two given HTML chunks that represent a *single* bus service.

        - `MAIN_LIST_HTML`: The summary div from the search results.
        - `DETAIL_TABLE_HTML`: The more detailed HTML from a sub-request.
        
        **Prioritize data from `DETAIL_TABLE_HTML`** as it is more accurate.
        Use `MAIN_LIST_HTML` as a *fallback* for fields not present in the detail
        table (like 'bus_type', 'seats_available', 'via_route').

        You MUST respond ONLY with a single, valid JSON object that strictly adheres to the
        provided JSON schema. Do not include any other text, explanations, or markdown
        code fences (like ```json).

        JSON SCHEMA:
        {self.bus_schema}
        """

    def _build_user_prompt(self, main_list_html: str, detail_table_html: str) -> str:
        """Builds the final user prompt with the two HTML chunks."""
        return f"""
        Please extract all available data from the HTML chunks below, following the
        JSON schema provided in the system prompt.

        Pay close attention to field types and descriptions:
        - "operator": The name of the bus corporation (e.g., "SALEM").
        - "bus_type": The class of bus (e.g., "AC 3X2").
        - "departure_time" and "arrival_time": Must be in "HH:MM" 24-hour format.
        - "duration": The journey time, e.g., "7.45" or "7:30".
        - "price_in_rs": The *integer* price (e.g., 350).
        - "seats_available": The *integer* number of seats.
        - "via_route": A list of stop names, or null if not present.
        - If a value is not found, set it to a sensible default (like 0, "N/A", or null).
        - **REMEMBER**: Prioritize `DETAIL_TABLE_HTML` first.

        MAIN_LIST_HTML (Fallback):
        {main_list_html}

        ---

        DETAIL_TABLE_HTML (Primary Source):
        {detail_table_html}
        """

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(3),
        reraise=True
    )
    async def _parse_chunk_with_ollama(
        self, 
        client: httpx.AsyncClient, 
        main_list_html: str,
        detail_table_html: str,
        bus_index: int
    ) -> Optional[BusService]:
        """
        Sends a single HTML chunk to the Ollama API for parsing and validation.
        Includes retry logic.
        """
        log.debug(f"OllamaParser: Parsing chunk {bus_index}...")
        user_prompt = self._build_user_prompt(main_list_html, detail_table_html)
        
        payload = {
            "model": OLLAMA_MODEL,
            "system": self.system_prompt,
            "prompt": user_prompt,
            "format": "json",
            "stream": False
        }
        
        try:
            response = await client.post(OLLAMA_API_URL, json=payload, timeout=130.0)
            response.raise_for_status()
            
            raw_json_string = response.json().get("response")
            
            if not raw_json_string:
                log.warning(f"OllamaParser: Bus {bus_index}: Received empty response.")
                return None
                
            parsed_data = json.loads(raw_json_string)
            bus_service = BusService(**parsed_data)
            return bus_service
            
        except httpx.HTTPStatusError as e:
            log.error(f"OllamaParser: Bus {bus_index}: API returned status {e.response.status_code}. {e.response.text}")
            raise
        except httpx.RequestError as e:
            log.error(f"OllamaParser: Bus {bus_index}: Network error calling Ollama: {e}")
            raise
        except json.JSONDecodeError as e:
            log.error(f"OllamaParser: Bus {bus_index}: Failed to decode JSON from LLM: {e}. Response was: {raw_json_string}")
            return None
        except ValidationError as e:
            log.error(f"OllamaParser: Bus {bus_index}: LLM output failed Pydantic validation: {e}. Data was: {parsed_data}")
            return None
        except Exception as e:
            log.error(f"OllamaParser: Bus {bus_index}: Unexpected error: {e}")
            return None

    async def _wrapper_parse_chunk(
            self, 
            semaphore: asyncio.Semaphore, 
            client: httpx.AsyncClient, 
            main_list_html: str, 
            detail_table_html: str,
            idx: int
        ) -> Optional[BusService]:
            """
            A wrapper that acquires the semaphore before calling the
            parsing function.
            """
            async with semaphore:
                log.debug(f"OllamaParser: [Semaphore Acquired] Parsing chunk {idx}...")
                try:
                    return await self._parse_chunk_with_ollama(client, main_list_html, detail_table_html, idx)
                finally:
                    # The semaphore is automatically released here
                    log.debug(f"OllamaParser: [Semaphore Released] Finished chunk {idx}.")

    async def _call_load_trip_details(self, client: httpx.AsyncClient, onclick_attr: str) -> str:
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
            log.error(f"Network error calling loadTripDetails: {e}")
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
        
        If 'limit' is provided, it will only process the first 'n' buses.
        """
        
        log.info(f"Using OllamaParser with model {OLLAMA_MODEL}...")
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
        for bus_div in bus_divs:
            a_tag = bus_div.find("a", attrs={"data-target": "#TripcodePopUp", "onclick": True})
            onclick_attr = a_tag.get("onclick", "") if a_tag else ""

            if onclick_attr:
                detail_tasks.append(self._call_load_trip_details(client, str(onclick_attr)))
            else:
                detail_tasks.append(asyncio.sleep(0, result="")) # Keep list aligned
        
        all_details_html = await asyncio.gather(*detail_tasks)

        # 2. Create tasks to parse each bus using the two HTML sources
        tasks = []
        for idx, bus_div in enumerate(bus_divs):
            main_list_html = str(bus_div)
            detail_table_html = all_details_html[idx]
            tasks.append(
                self._wrapper_parse_chunk(
                    semaphore, 
                    client, 
                    main_list_html, 
                    detail_table_html, 
                    idx
                )
            )
        
        results = await asyncio.gather(*tasks)
        
        bus_services: List[BusService] = [service for service in results if service is not None]
        
        log.info(f"OllamaParser: Successfully parsed {len(bus_services)} / {len(bus_divs)} bus services.")
        
        return bus_services