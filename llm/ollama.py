import httpx
import logging
import asyncio
import ollama
import json
from typing import List, Optional
from bs4 import BeautifulSoup
from pydantic import ValidationError
from tenacity import wait_exponential, stop_after_attempt, Retrying

from .interface import AbstractBusParser
from .prompts import PromptGenerator
from tnstc_api.schemas import BusService
from tnstc_api.config import OLLAMA_MODEL, OLLAMA_CONCURRENCY_LIMIT, OLLAMA_BASE_URL
from utils.helpers import minify_html, calculate_message_size
from utils.logging_setup import setup_logging

setup_logging()
log = logging.getLogger(__name__)

class OllamaParser(AbstractBusParser):
    def __init__(self):
        try:
            self.client = ollama.AsyncClient(host=OLLAMA_BASE_URL)
            self.model = OLLAMA_MODEL
            self.prompt_gen = PromptGenerator()
            
            self.system_prompt = self.prompt_gen.build_system_prompt(BusService)
            
            self.total_chars_sent = 0
            self.total_requests = 0
            log.info(f"OllamaParser initialized. Model: {self.model}. Base URL: {OLLAMA_BASE_URL}")
        except Exception as e:
            log.error(f"Failed to initialize Ollama client: {e}")
            raise

    async def _parse_chunk_with_ollama(self, main_list_html: str, detail_table_html: str, bus_index: int) -> Optional[BusService]:
        
        user_content = self.prompt_gen.build_user_message(main_list_html, detail_table_html)
        
        messages = [
            {'role': 'system', 'content': self.system_prompt},
            {'role': 'user', 'content': user_content}
        ]
        
        retry_config = Retrying(
            wait=wait_exponential(multiplier=1, min=2, max=30),
            stop=stop_after_attempt(3),
            reraise=True
        )

        for attempt in retry_config:
            with attempt:
                message_size = calculate_message_size(messages)
                self.total_chars_sent += message_size
                self.total_requests += 1

                log.info(f"OllamaParser Bus {bus_index} (Attempt {attempt.retry_state.attempt_number}): Sending {message_size} chars.")

                try:
                    response = await self.client.chat(
                        model=self.model,
                        messages=messages,                        
                        format='json',
                        options={'temperature': 0.0}
                    )
                    json_content = response['message']['content']
                    return BusService.model_validate_json(json_content)
                except (json.JSONDecodeError, ValidationError) as e:
                    log.error(f"OllamaParser Bus {bus_index}: Parsing failed: {e}")
                    raise
                except Exception as e:
                    log.error(f"OllamaParser Bus {bus_index}: Client error: {e}")
                    raise

    async def _wrapper_parse_chunk(self, semaphore: asyncio.Semaphore, main_html: str, detail_html: str, idx: int) -> Optional[BusService]:
        async with semaphore:
            return await self._parse_chunk_with_ollama(main_html, detail_html, idx)

    async def parse(self, client: httpx.AsyncClient, html_content: str, limit: Optional[int] = None) -> List[BusService]:
        log.info(f"Using OllamaParser with concurrency {OLLAMA_CONCURRENCY_LIMIT}...")
        semaphore = asyncio.Semaphore(OLLAMA_CONCURRENCY_LIMIT)

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
            tasks.append(self._wrapper_parse_chunk(semaphore, main, detail, idx))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return [res for res in results if isinstance(res, BusService)]