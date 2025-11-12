import httpx
from typing import List, Optional
import json
import logging
from pydantic import ValidationError
from tenacity import retry, wait_exponential, stop_after_attempt
import asyncio
import re
from bs4 import BeautifulSoup

from ..schemas import BusService, get_gemini_schema_for
from ..config import GEMINI_API_KEY, GEMINI_API_URL, TNSTC_DETAILS_URL

log = logging.getLogger(__name__)

class GeminiParser:
    """
    Implements the BusParser interface using the Gemini API's structured output (JSON mode).
    """

    def __init__(self):
        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY environment variable is not set. Cannot use GeminiParser.")
        
        self.api_url = f"{GEMINI_API_URL}?key={GEMINI_API_KEY}"
        
        self.service_schema = get_gemini_schema_for(BusService)
        
        self.system_prompt = f"""
        You are an expert, automated HTML parsing engine. Your sole task is to extract
        bus service details from the provided HTML content and return them as a
        single, valid JSON object.

        - You will be given two HTML snippets:
          1. `MAIN_LIST_HTML`: The summary div from the search results.
          2. `DETAIL_TABLE_HTML`: The more detailed HTML from a sub-request.
        - You MUST adhere *strictly* to the provided JSON schema.
        - **Prioritize data from `DETAIL_TABLE_HTML`** as it is more accurate.
        - Use `MAIN_LIST_HTML` as a *fallback* for fields not present in the detail
          table (like 'bus_type', 'seats_available', 'via_route').
        - Pay close attention to data types (e.g., 'price_in_rs' must be an integer).
        - Do NOT include any text, explanations, or markdown fences (```json) in your response.
        - Your entire response must be *only* the JSON object.
        """

    async def _call_gemini_api_internal(
        self, 
        client: httpx.AsyncClient, 
        payload: dict
    ) -> BusService:
        """
        Internal method to call the Gemini API. Not retryable on its own.
        """
        log.debug("Calling Gemini API...")
        
        response = await client.post(self.api_url, json=payload, timeout=120.0)        
        response.raise_for_status() 
        
        result = response.json()

        try:
            candidate = result.get('candidates', [])[0]
            json_text = candidate['content']['parts'][0]['text']
            
            data = json.loads(json_text)            
            return BusService(**data)
        
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            log.error(f"GeminiParser: Failed to parse valid JSON from LLM response. Error: {e}. Response: {result}")
            raise ValueError(f"Failed to parse LLM JSON response: {e}")
        except ValidationError as e:
            log.error(f"GeminiParser: LLM output failed Pydantic validation. Error: {e}. Data: {data}")
            raise
            
    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5),
        reraise=True
    )
    async def _parse_bus_with_gemini(
        self,
        client: httpx.AsyncClient,
        main_list_html: str,
        detail_table_html: str,
        bus_index: int
    ) -> Optional[BusService]:
        """
        Parses a single bus by sending its two HTML sources to Gemini.
        This method is retryable.
        """
        log.debug(f"GeminiParser: Parsing bus {bus_index}...")

        user_prompt = f"""
        Please extract all bus service details from the following HTML snippets.
        Prioritize `DETAIL_TABLE_HTML` and use `MAIN_LIST_HTML` as a fallback.
        Return the data as a JSON object matching the system prompt's schema.

        MAIN_LIST_HTML (Fallback):
        {main_list_html}
        
        ---

        DETAIL_TABLE_HTML (Primary Source):
        {detail_table_html}
        """

        payload = {
            "contents": [{"parts": [{"text": user_prompt}]}],
            "systemInstruction": {"parts": [{"text": self.system_prompt}]},
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": self.service_schema
            }
        }
        
        try:
            service = await self._call_gemini_api_internal(client, payload)
            return service
        except Exception as e:
            log.error(f"GeminiParser: Bus {bus_index}: Failed after retries. Error: {e}")
            raise

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
        html_content: str
    ) -> List[BusService]:
        """
        Parses the main HTML by finding each bus, triggering its detail
        sub-request, and then parsing each bus individually using Gemini.
        """
        log.info(f"Using GeminiParser to parse bus results (hybrid strategy)...")
        
        soup = BeautifulSoup(html_content, 'lxml')
        bus_divs = soup.find_all('div', class_ = 'bus-list')
        
        if not bus_divs:
            log.warning("GeminiParser: No 'div.bus-list' elements found in HTML.")
            return []

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
        parsing_tasks = []
        for idx, bus_div in enumerate(bus_divs):
            main_list_html = str(bus_div)
            detail_table_html = all_details_html[idx]
            
            parsing_tasks.append(
                self._parse_bus_with_gemini(
                    client, 
                    main_list_html, 
                    detail_table_html, 
                    idx
                )
            )
        
        # 3. Gather all parsing results
        results = await asyncio.gather(*parsing_tasks, return_exceptions=True)
        
        bus_services: List[BusService] = []
        for idx, res in enumerate(results):
            if isinstance(res, BusService):
                bus_services.append(res)
            elif isinstance(res, Exception):
                log.error(f"GeminiParser: Bus {idx}: Failed final parsing attempt. Error: {res}")

        log.info(f"GeminiParser: Successfully parsed {len(bus_services)} / {len(bus_divs)} bus services.")
        return bus_services