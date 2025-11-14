import httpx
from typing import List, Optional
import logging
from tenacity import wait_exponential, stop_after_attempt, Retrying
import asyncio
import re
from bs4 import BeautifulSoup
from pydantic import ValidationError

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, SystemMessage

from .prompt_builder import PromptGenerator

from ..schemas import BusService, BusServiceWithReasoning
from ..config import GEMINI_API_KEY, GEMINI_MODEL, TNSTC_DETAILS_URL, GEMINI_LOAD_TIMEOUT

log = logging.getLogger(__name__)

class GeminiParser:
    """
    Implements the BusParser interface using the LangChain Google Generative AI
    model with its native structured output feature.
    """

    def __init__(self):
        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY environment variable is not set. Cannot use GeminiParser.")
        
        try:
            self.llm = ChatGoogleGenerativeAI(
                model=GEMINI_MODEL, 
                api_key=GEMINI_API_KEY,
                request_timeout=GEMINI_LOAD_TIMEOUT
            )

            self.prompt_gen = PromptGenerator()

            self.structured_llm = self.llm.with_structured_output(BusService)
        except ImportError:
            log.error("LangChain Google GENAI library not found. Please install 'langchain-google-genai'")
            raise
        except Exception as e:
            log.error(f"Failed to initialize Gemini LLM: {e}")
            raise
        
        self.system_prompt = self.prompt_gen.build_system_prompt(BusService)
            
    async def _parse_bus_with_langchain(
        self,
        main_list_html: str,
        detail_table_html: str,
        bus_index: int
    ) -> Optional[BusService]:
        """
        Parses a single bus by sending its two HTML sources to Gemini.
        Returns the clean BusService object (without reasoning field).
        """

        user_prompt = f"""
        MAIN_LIST_HTML
        {main_list_html}
        
        ---

        DETAIL_TABLE_HTML
        {detail_table_html}

        TASK:
        Extract every available field defined in the JSON_SCHEMA from these HTML fragments.

        ---
        Extraction Hints (Follow these carefully):
        1.  trip_code: Find the <a> tag. The trip_code is the text inside it. extract the text inside MAIN_LIST_HTML <b><a>...</a></b> (trim whitespace). If not found there, check DETAIL_TABLE_HTML.
            (e.g., from `<a> 0005SALMADMM01L</a>`, the trip_code is "0005SALMADMM01L").
        2.  route_code: This is the value usually (not everytime though) immediately after the " / " separator. often follows the trip code or appears near it; check MAIN_LIST_HTML first.
            (e.g., from `...</a></b> / 104N1`, the route_code is "104N1").
        3.  via_route: Find the text "Via-". The value is a list of the places that follow. 
            (e.g., from `Via-KARUR , DINDIGUL`, the via_route is ["KARUR", "DINDIGUL"]).
        4.  trip_code vs route_code: They are different fields. Do not confuse them. 
            trip_code is the long one (0005SALMADMM01L), route_code is the short one (104N1).
        5.  duration: Use the value ending in "Hrs" (e.g., "6.10Hrs" becomes "6.10"). return a normalized float-string in hours with 2 decimals. (6h10m -> "6.17")
        6. price and seats: prefer MAIN_LIST_HTML, use details list as fallback if not found.
        7. total_kms: Look in the `DETAIL_TABLE_HTML` for a label like "Approx. Kms" or "Total Kms" or something similar and extract the numeric value next to it (e.g., "253.00").
        8. If a value is not found, return "NA".
        9. Return only the JSON object, nothing else.

        Trip code pattern hint: look for the longest contiguous alphanumeric uppercase token of length >=8 (e.g., 0005SALMADMM01L).

        ---

        Return:
        → A single JSON object that conforms exactly to the JSON_SCHEMA provided in the system prompt.
        → Do not include any extra text, comments, or markdown.
        → If a value is not found, return "NA" for that field (or `null` for `via_route`).
        → Output strictly raw JSON.
        """

        messages = [
            SystemMessage(content=self.system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        retry_config = Retrying(
            wait=wait_exponential(multiplier=1, min=2, max=60),
            stop=stop_after_attempt(5),
            reraise=True
        )

        for attempt in retry_config:
            with attempt:
                log.info(f"LLM_Parser Bus {bus_index} (Attempt {attempt.retry_state.attempt_number}): Sending HTML (Main: {len(main_list_html)} chars, Detail: {len(detail_table_html)} chars) to LLM for structured extraction.") 
                
                try:
                    service_with_reasoning = await self.structured_llm.ainvoke(messages)

                    if isinstance(service_with_reasoning, BusServiceWithReasoning):
                        
                        log.info(f"LLM_Parser Bus {bus_index} SUCCESS: Extracted details for '{service_with_reasoning.operator}' (Price: {service_with_reasoning.price_in_rs}, Trip: {service_with_reasoning.trip_code}).") 
                        if service_with_reasoning.llm_reasoning:
                            log.info(f"LLM Reasoning for Bus {bus_index}: {service_with_reasoning.llm_reasoning}")
                        
                        return BusService.model_validate(service_with_reasoning.model_dump())
                    else:
                        log.error(f"GeminiParser: Bus {bus_index}: LangChain returned unexpected type: {type(service_with_reasoning)}")
                        raise TypeError("LLM returned wrong type")
                
                except ValidationError as e:
                    log.error(f"LLM_Parser Bus {bus_index}: Pydantic validation failed. Input: '{user_prompt[:50]}...'. Error: {e}", exc_info=True)
                    raise
                except Exception as e:
                    log.error(f"GeminiParser: Bus {bus_index}: Failed during LangChain invocation: {e}")
                    raise


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
        sub-request, and then parsing each bus individually using Gemini.
        """
        log.info(f"Using GeminiParser to parse bus results (LangChain strategy)...")
        
        soup = BeautifulSoup(html_content, 'lxml')
        bus_divs = soup.find_all('div', class_ = 'bus-list')
        
        if not bus_divs:
            log.warning("GeminiParser: No 'div.bus-list' elements found in HTML.")
            return []

        if limit is not None:
            log.info(f"GeminiParser: Applying limit of {limit} buses.")
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
                log.warning(f"GeminiParser Bus {idx}: No 'onclick' attribute found. Cannot fetch details.")

        log.info(f"GeminiParser: Awaiting concurrent detail fetch for {len(detail_tasks)} buses...")
        all_details_html = await asyncio.gather(*detail_tasks)

        # 2. Create tasks to parse each bus using the two HTML sources
        parsing_tasks = []
        for idx, bus_div in enumerate(bus_divs):
            main_list_html = str(bus_div)
            detail_table_html = all_details_html[idx]
            
            parsing_tasks.append(
                self._parse_bus_with_langchain(
                    main_list_html, 
                    detail_table_html, 
                    idx
                )
            )
        
        # 3. Gather all parsing results
        log.info(f"GeminiParser: Awaiting concurrent LLM parsing for {len(parsing_tasks)} buses...")
        results = await asyncio.gather(*parsing_tasks, return_exceptions=True)
        
        bus_services: List[BusService] = []
        for idx, res in enumerate(results):
            if isinstance(res, BusService):
                bus_services.append(res)
            elif isinstance(res, Exception):
                log.error(f"GeminiParser: Bus {idx}: Failed final parsing attempt after retries. Error: {res}")

        log.info(f"GeminiParser: Successfully parsed {len(bus_services)} / {len(bus_divs)} bus services.")
        return bus_services