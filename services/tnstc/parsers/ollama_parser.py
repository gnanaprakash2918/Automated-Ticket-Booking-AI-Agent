import asyncio
import logging
from typing import List, Optional
from bs4 import BeautifulSoup
import httpx
from tenacity import Retrying, stop_after_attempt, wait_exponential
from llm.interface import LLMInterface
from llm.ollama import OllamaLLM
from utils.helpers import minify_html
from utils.logger import setup_logging
from ..config import OLLAMA_CONCURRENCY_LIMIT
from ..schemas import TNSTCBusService
from .base import AbstractBusParser


setup_logging()
log = logging.getLogger(__name__)


class OllamaParser(AbstractBusParser):
    """
    Implements the BusParser interface using the OllamaLLM adapter.
    Utilizes LLMInterface for prompt management and handles local concurrency limits.
    """

    def __init__(self):
        try:
            self.llm: LLMInterface = OllamaLLM(prompt_dir="../prompts")

            self.system_prompt = self.llm.construct_system_prompt(
                schema=TNSTCBusService, filename="system_prompt.txt"
            )

            self.total_requests = 0

        except Exception as e:
            log.error(f"Failed to initialize Ollama LLM Adapter: {e}")
            raise

    async def _parse_bus_with_llm(
        self, main_list_html: str, detail_table_html: str, bus_index: int
    ) -> Optional[TNSTCBusService]:
        """
        Parses a single bus.
        Uses LLMInterface.construct_user_prompt to merge HTML into the template.
        """

        try:
            user_prompt = self.llm.construct_user_prompt(
                main_html=main_list_html,
                detail_html=detail_table_html,
                filename="user_prompt.txt",
            )
        except FileNotFoundError as e:
            log.critical(f"Missing prompt file: {e}")
            raise

        retry_config = Retrying(
            wait=wait_exponential(multiplier=1, min=2, max=30),
            stop=stop_after_attempt(3),
            reraise=True,
        )

        for attempt in retry_config:
            with attempt:
                self.total_requests += 1
                log.debug(
                    f"Bus {bus_index}: Sending to Ollama (Attempt {attempt.retry_state.attempt_number})"
                )

                try:
                    bus_service = await self.llm.generate_structured(
                        schema=TNSTCBusService,
                        prompt=user_prompt,
                        system_prompt=self.system_prompt,
                        temperature=0.0,
                    )

                    log.info(
                        f"Bus {bus_index} SUCCESS: {bus_service.operator} - {bus_service.trip_code}"
                    )
                    return bus_service

                except Exception as e:
                    log.warning(
                        f"Bus {bus_index}: Ollama parsing failed on attempt {attempt.retry_state.attempt_number}: {e}"
                    )
                    raise

    async def _wrapper_parse_chunk(
        self,
        semaphore: asyncio.Semaphore,
        main_list_html: str,
        detail_table_html: str,
        idx: int,
    ) -> Optional[TNSTCBusService]:
        """
        Wrapper to strictly limit concurrent Ollama requests using a Semaphore.
        Crucial for local LLM inference stability.
        """

        async with semaphore:
            log.debug(
                f"OllamaParser: [SEMAPHORE ACQUIRED] Bus {idx}. Active slots: {OLLAMA_CONCURRENCY_LIMIT - semaphore._value}"
            )
            try:
                return await self._parse_bus_with_llm(
                    main_list_html, detail_table_html, idx
                )
            finally:
                log.debug(f"OllamaParser: [SEMAPHORE RELEASED] Bus {idx}.")

    async def parse(
        self, client: httpx.AsyncClient, html_content: str, limit: Optional[int] = None
    ) -> List[TNSTCBusService]:
        """
        Orchestration: Finds divs, fetches details, parses concurrently (throttled).
        """
        log.info(
            f"Using OllamaParser (Interface Loader strategy). Concurrency Limit: {OLLAMA_CONCURRENCY_LIMIT}"
        )

        semaphore = asyncio.Semaphore(OLLAMA_CONCURRENCY_LIMIT)

        soup = BeautifulSoup(html_content, "lxml")
        bus_divs = soup.find_all("div", class_="bus-list")

        if not bus_divs:
            log.warning("OllamaParser: No 'div.bus-list' elements found.")
            return []

        if limit:
            log.info(f"OllamaParser: Applying limit of {limit} buses.")
            bus_divs = bus_divs[:limit]

        all_details_html = []
        log.info(f"OllamaParser: Fetching details for {len(bus_divs)} buses...")

        for idx, bus_div in enumerate(bus_divs):
            a_tag = bus_div.find(
                "a", attrs={"data-target": "#TripcodePopUp", "onclick": True}
            )
            onclick_attr = a_tag.get("onclick", "") if a_tag else ""

            if onclick_attr:
                detail_html = await self._call_load_trip_details(
                    client, str(onclick_attr), idx
                )
                all_details_html.append(detail_html)
            else:
                log.warning(f"Bus {idx}: No onclick attribute found.")
                all_details_html.append("")

        parsing_tasks = []
        for idx, bus_div in enumerate(bus_divs):
            main_clean = minify_html(str(bus_div))
            detail_clean = minify_html(all_details_html[idx])

            parsing_tasks.append(
                self._wrapper_parse_chunk(semaphore, main_clean, detail_clean, idx)
            )

        log.info(f"OllamaParser: Queued {len(parsing_tasks)} tasks. Processing...")
        results = await asyncio.gather(*parsing_tasks, return_exceptions=True)

        bus_services = []
        for idx, res in enumerate(results):
            if isinstance(res, TNSTCBusService):
                bus_services.append(res)
            elif isinstance(res, Exception):
                log.error(f"Bus {idx}: Final parsing error: {res}")

        log.info(
            f"OllamaParser: Completed. {len(bus_services)}/{len(bus_divs)} parsed successfully."
        )

        return bus_services
