import httpx
import logging
import asyncio
from typing import List, Optional, Any
from pydantic import BaseModel
from bs4 import BeautifulSoup
from pydantic import ValidationError
from tenacity import wait_exponential, stop_after_attempt, Retrying

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, SystemMessage

from .interface import AbstractBusParser
from .prompts import PromptGenerator
from tnstc_api.schemas import BusService
from tnstc_api.config import GEMINI_API_KEY, GEMINI_MODEL, GEMINI_LOAD_TIMEOUT
from utils.helpers import minify_html
from utils.logging_setup import setup_logging

setup_logging()
log = logging.getLogger(__name__)

class GeminiParser(AbstractBusParser):
    def __init__(self):
        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY not set.")
        
        try:
            self.llm = ChatGoogleGenerativeAI(
                model=GEMINI_MODEL, 
                api_key=GEMINI_API_KEY,
                request_timeout=GEMINI_LOAD_TIMEOUT,
                temperature=0.0
            )

            self.prompt_gen = PromptGenerator()
            self.structured_llm = self.llm.with_structured_output(BusService)            
            self.system_prompt = self.prompt_gen.build_system_prompt(BusService)
            
            self.total_chars_sent = 0
            self.total_requests = 0
            
        except Exception as e:
            log.error(f"Failed to initialize Gemini LLM: {e}")
            raise

    async def _parse_bus_with_langchain(self, main_list_html: str, detail_table_html: str, bus_index: int) -> Optional[BusService]:
        
        user_content = self.prompt_gen.build_user_message(main_list_html, detail_table_html)

        messages = [
            SystemMessage(content=self.system_prompt),
            HumanMessage(content=user_content)
        ]
        
        retry_config = Retrying(
            wait=wait_exponential(multiplier=1, min=2, max=60),
            stop=stop_after_attempt(5),
            reraise=True
        )

        for attempt in retry_config:
            with attempt:
                message_size = sum(len(m.content) for m in messages)
                self.total_chars_sent += message_size
                self.total_requests += 1

                log.info(f"GeminiParser Bus {bus_index} (Attempt {attempt.retry_state.attempt_number}): Sending {message_size} chars.") 
                
                try:
                    service: Any = await self.structured_llm.ainvoke(messages)
                    data = service.model_dump() if isinstance(service, BaseModel) else service

                    return BusService.model_validate(data)
                
                except ValidationError as e:
                    log.error(f"GeminiParser Bus {bus_index}: Validation failed: {e}")
                    raise
                except Exception as e:
                    log.error(f"GeminiParser Bus {bus_index}: Invocation failed: {e}")
                    raise

    async def parse(self, client: httpx.AsyncClient, html_content: str, limit: Optional[int] = None) -> List[BusService]:
        log.info("Using GeminiParser...")

        soup = BeautifulSoup(html_content, 'lxml')
        bus_divs = soup.find_all('div', class_='bus-list')
        
        if not bus_divs: return []
        if limit: bus_divs = bus_divs[:limit]

        all_details_html = []
        log.info(f"Fetching details for {len(bus_divs)} buses...")
        for idx, bus_div in enumerate(bus_divs):
            a_tag = bus_div.find("a", attrs={"data-target": "#TripcodePopUp", "onclick": True})
            onclick = a_tag.get("onclick", "") if a_tag else ""
            
            all_details_html.append(await self._call_load_trip_details(client, str(onclick), idx) if onclick else "")

        tasks = []
        for idx, bus_div in enumerate(bus_divs):
            main = minify_html(str(bus_div))
            detail = minify_html(all_details_html[idx])
            tasks.append(self._parse_bus_with_langchain(main, detail, idx))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [res for res in results if isinstance(res, BusService)]