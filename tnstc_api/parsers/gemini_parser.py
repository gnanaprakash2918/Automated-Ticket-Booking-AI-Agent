import httpx
from typing import List, Optional
import logging
from tenacity import retry, wait_exponential, stop_after_attempt
import asyncio
import re
from bs4 import BeautifulSoup
from pydantic import ValidationError

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig

from ..schemas import BusService
from ..config import GEMINI_API_KEY, GEMINI_MODEL, TNSTC_DETAILS_URL

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
            self.llm = ChatGoogleGenerativeAI(model=GEMINI_MODEL, api_key=GEMINI_API_KEY)
            self.structured_llm = self.llm.with_structured_output(BusService)
        except ImportError:
            log.error("LangChain Google GENAI library not found. Please install 'langchain-google-genai'")
            raise
        except Exception as e:
            log.error(f"Failed to initialize Gemini LLM: {e}")
            raise
        
        self.system_prompt = f"""
        You are an expert, automated HTML parsing engine. Your sole task is to extract
        bus service details from the provided HTML content.

        - You will be given two HTML snippets:
          1. `MAIN_LIST_HTML`: The summary div from the search results.
          2. `DETAIL_TABLE_HTML`: The more detailed HTML from a sub-request.
        - **Prioritize data from `DETAIL_TABLE_HTML`** as it is more accurate.
        - Use `MAIN_LIST_HTML` as a *fallback* for fields not present in the detail
          table (like 'bus_type', 'seats_available', 'via_route').
        """
            
    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5),
        reraise=True
    )
    async def _parse_bus_with_langchain(
        self,
        client: httpx.AsyncClient,
        main_list_html: str,
        detail_table_html: str,
        bus_index: int
    ) -> Optional[BusService]:
        """
        Parses a single bus by sending its two HTML sources to Gemini
        using LangChain's structured output. This method is retryable.
        """
        log.debug(f"GeminiParser: Parsing bus {bus_index} with LangChain...")

        user_prompt = f"""
        Please extract all bus service details from the following HTML snippets.
        Prioritize `DETAIL_TABLE_HTML` and use `MAIN_LIST_HTML` as a fallback.

        MAIN_LIST_HTML (Fallback):
        {main_list_html}
        
        ---

        DETAIL_TABLE_HTML (Primary Source):
        {detail_table_html}
        """

        messages = [
            SystemMessage(content=self.system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        try:

            config = RunnableConfig(configurable={"request_timeout": 200.0})
            service = await self.structured_llm.ainvoke(messages, config=config)

            if isinstance(service, BusService):
                return service
            else:
                log.error(f"GeminiParser: Bus {bus_index}: LangChain returned unexpected type: {type(service)}")
                return None
        
        except ValidationError as e:
            log.error(f"GeminiParser: Bus {bus_index}: LLM output failed Pydantic validation. Error: {e}")
            raise
        except Exception as e:
            log.error(f"GeminiParser: Bus {bus_index}: Failed during LangChain invocation. Error: {e}")
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
        html_content: str,
        limit: Optional[int] = None
    ) -> List[BusService]:
        """
        Parses the main HTML by finding each bus, triggering its detail
        sub-request, and then parsing each bus individually using Gemini.
        
        If 'limit' is provided, it will only process the first 'n' buses.
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
                self._parse_bus_with_langchain(
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