import asyncio
import logging
from typing import List, Optional
from bs4 import BeautifulSoup
import httpx
from tenacity import Retrying, stop_after_attempt, wait_exponential
from llm.gemini import GeminiLLM
from llm.interface import LLMInterface
from utils.helpers import minify_html
from utils.logging_setup import setup_logging
from ..schemas import TNSTCBusService
from .base import AbstractBusParser


setup_logging()
log = logging.getLogger(__name__)

class GeminiParser(AbstractBusParser):
    """
    Implements the BusParser interface using the GeminiLLM adapter.
    Fully utilizes LLMInterface for prompt loading and construction.
    """

    def __init__(self):
        try:
            self.llm: LLMInterface = GeminiLLM(prompt_dir="../prompts")
            
            self.system_prompt = self.llm.construct_system_prompt(
                schema=TNSTCBusService,
                filename="system_prompt.txt"
            )
            
            self.total_requests = 0
            
        except Exception as e:
            log.error(f"Failed to initialize Gemini LLM Adapter: {e}")
            raise

    async def _parse_bus_with_llm(
        self,
        main_list_html: str,
        detail_table_html: str,
        bus_index: int
    ) -> Optional[TNSTCBusService]:
        """
        Parses a single bus. 
        Uses LLMInterface.construct_user_prompt to merge HTML into the template.
        """

        try:
            user_prompt = self.llm.construct_user_prompt(
                main_html=main_list_html,
                detail_html=detail_table_html,
                filename="user_prompt.txt"
            )

        except FileNotFoundError as e:
            log.critical(f"Missing prompt file: {e}")
            raise

        retry_config = Retrying(
            wait=wait_exponential(multiplier=1, min=2, max=60),
            stop=stop_after_attempt(4),
            reraise=True
        )

        for attempt in retry_config:
            with attempt:
                self.total_requests += 1
                log.debug(f"Bus {bus_index}: Sending to LLM (Attempt {attempt.retry_state.attempt_number})")
                
                try:
                    bus_service = await self.llm.generate_structured(
                        schema=TNSTCBusService,
                        prompt=user_prompt,
                        system_prompt=self.system_prompt
                    )
                    
                    return bus_service

                except Exception as e:
                    log.warning(f"Bus {bus_index}: Parsing failed on attempt {attempt.retry_state.attempt_number}: {e}")
                    raise

    async def parse(
        self, 
        client: httpx.AsyncClient, 
        html_content: str,
        limit: Optional[int] = None
    ) -> List[TNSTCBusService]:
        """
        Orchestration: Finds divs, fetches details, parses concurrently.
        """
        log.info("Using GeminiParser (Interface Loader strategy)...")
        
        soup = BeautifulSoup(html_content, 'lxml')
        bus_divs = soup.find_all('div', class_='bus-list')
        
        if not bus_divs:
            log.warning("GeminiParser: No 'div.bus-list' elements found.")
            return []

        if limit:
            bus_divs = bus_divs[:limit]

        all_details_html = []
        for idx, bus_div in enumerate(bus_divs):
            a_tag = bus_div.find("a", attrs={"data-target": "#TripcodePopUp", "onclick": True})
            onclick_attr = a_tag.get("onclick", "") if a_tag else ""

            if onclick_attr:
                detail_html = await self._call_load_trip_details(client, str(onclick_attr), idx)
                all_details_html.append(detail_html)
            else:
                all_details_html.append("")

        parsing_tasks = []
        for idx, bus_div in enumerate(bus_divs):
            main_clean = minify_html(str(bus_div))
            detail_clean = minify_html(all_details_html[idx])
            
            parsing_tasks.append(
                self._parse_bus_with_llm(main_clean, detail_clean, idx)
            )
        
        log.info(f"GeminiParser: Awaiting LLM results for {len(parsing_tasks)} buses...")
        results = await asyncio.gather(*parsing_tasks, return_exceptions=True)
        
        bus_services = []
        for res in results:
            if isinstance(res, TNSTCBusService):
                bus_services.append(res)
            elif isinstance(res, Exception):
                log.error(f"Parsing Error: {res}")

        return bus_services