import asyncio
from typing import List, Optional
from bs4 import BeautifulSoup
import httpx
from loguru import logger
from tenacity import Retrying, stop_after_attempt, wait_exponential
from llm.interface import LLMInterface
from utils.helpers import minify_html
from ..config import OLLAMA_CONCURRENCY_LIMIT
from ..schemas import TNSTCBusService
from .base import AbstractBusParser


class OllamaParser(AbstractBusParser):
    """
    Implements the BusParser interface using the OllamaLLM adapter.
    """

    def __init__(self):
        try:
            logger.info("Initializing OllamaParser with LLMFactory...")
            from llm.factory import LLMFactory

            self.llm: LLMInterface = LLMFactory.create_llm(
                provider="ollama", prompt_dir="services/tnstc/prompts"
            )

            self.system_prompt = self.llm.construct_system_prompt(
                schema=TNSTCBusService, filename="system_prompt.txt"
            )
            self.total_requests = 0
            logger.info(
                f"OllamaParser initialized successfully. Concurrency limit: {OLLAMA_CONCURRENCY_LIMIT}"
            )

        except Exception as e:
            logger.error(f"Failed to initialize Ollama LLM Adapter: {e}")
            raise

    async def _parse_bus_with_llm(
        self, main_list_html: str, detail_table_html: str, bus_index: int
    ) -> Optional[TNSTCBusService]:
        logger.debug(
            f"Bus {bus_index}: Constructing user prompt "
            f"(main_html_len={len(main_list_html)}, detail_html_len={len(detail_table_html)})"
        )

        try:
            user_prompt = self.llm.construct_user_prompt(
                main_html=main_list_html,
                detail_html=detail_table_html,
                filename="user_prompt.txt",
            )
        except FileNotFoundError as e:
            logger.critical(f"Bus {bus_index}: Missing prompt file: {e}")
            raise

        retry_config = Retrying(
            wait=wait_exponential(multiplier=1, min=2, max=30),
            stop=stop_after_attempt(3),
            reraise=True,
        )

        for attempt in retry_config:
            with attempt:
                self.total_requests += 1
                logger.info(
                    f"Bus {bus_index}: Sending to Ollama "
                    f"(Attempt {attempt.retry_state.attempt_number}, total_requests={self.total_requests})"
                )

                try:
                    bus_service = await self.llm.generate_structured(
                        schema=TNSTCBusService,
                        prompt=user_prompt,
                        system_prompt=self.system_prompt,
                        temperature=0.0,
                    )

                    return bus_service

                except Exception as e:
                    logger.warning(
                        f"Bus {bus_index}: Ollama parsing failed on attempt "
                        f"{attempt.retry_state.attempt_number}: {e}"
                    )
                    raise

    async def _wrapper_parse_chunk(
        self,
        semaphore: asyncio.Semaphore,
        main_list_html: str,
        detail_table_html: str,
        idx: int,
    ) -> Optional[TNSTCBusService]:
        async with semaphore:
            try:
                result = await self._parse_bus_with_llm(
                    main_list_html, detail_table_html, idx
                )
                return result
            finally:
                logger.debug(f"OllamaParser: [SEMAPHORE RELEASED] Bus {idx}.")

    async def parse_buses(
        self,
        client: httpx.AsyncClient,
        bus_html_list: List[str],
        limit: Optional[int] = None,
    ) -> List[TNSTCBusService]:
        logger.info(
            f"OllamaParser.parse_buses: Processing {len(bus_html_list)} "
            f"pre-filtered bus HTML snippets"
        )

        if limit is not None and len(bus_html_list) > limit:
            logger.info(f"OllamaParser: Limiting processing to first {limit} items.")
            bus_html_list = bus_html_list[:limit]

        semaphore = asyncio.Semaphore(OLLAMA_CONCURRENCY_LIMIT)
        all_details_html = []

        for idx, bus_html in enumerate(bus_html_list):
            soup = BeautifulSoup(bus_html, "lxml")
            bus_div = soup.find("div", class_="bus-list")

            if not bus_div:
                all_details_html.append("")
                continue

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
                all_details_html.append("")

        parsing_tasks = []
        for idx, bus_html in enumerate(bus_html_list):
            main_clean = minify_html(bus_html)
            detail_clean = minify_html(all_details_html[idx])
            parsing_tasks.append(
                self._wrapper_parse_chunk(semaphore, main_clean, detail_clean, idx)
            )

        logger.info(f"OllamaParser: Queued {len(parsing_tasks)} tasks. Processing...")
        results = await asyncio.gather(*parsing_tasks, return_exceptions=True)

        bus_services = []
        current_bus_num = 1
        for idx, res in enumerate(results):
            if isinstance(res, TNSTCBusService):
                res.bus_number = current_bus_num
                bus_services.append(res)
                current_bus_num += 1
            elif isinstance(res, Exception):
                logger.error(f"Bus {idx}: Final parsing error: {res}")

        logger.info(
            f"OllamaParser.parse_buses: Completed. {len(bus_services)} parsed successfully."
        )
        return bus_services

    async def parse(
        self, client: httpx.AsyncClient, html_content: str, limit: Optional[int] = None
    ) -> List[TNSTCBusService]:
        logger.info(
            f"Using OllamaParser (Interface Loader strategy). "
            f"Concurrency Limit: {OLLAMA_CONCURRENCY_LIMIT}"
        )

        semaphore = asyncio.Semaphore(OLLAMA_CONCURRENCY_LIMIT)
        soup = BeautifulSoup(html_content, "lxml")
        bus_divs = soup.find_all("div", class_="bus-list")

        if not bus_divs:
            return []

        if limit is not None and len(bus_divs) > limit:
            logger.info(f"OllamaParser: Limiting processing to first {limit} buses.")
            bus_divs = bus_divs[:limit]

        all_details_html = []
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
                all_details_html.append("")

        parsing_tasks = []
        for idx, bus_div in enumerate(bus_divs):
            main_clean = minify_html(str(bus_div))
            detail_clean = minify_html(all_details_html[idx])
            parsing_tasks.append(
                self._wrapper_parse_chunk(semaphore, main_clean, detail_clean, idx)
            )

        logger.info(f"OllamaParser: Queued {len(parsing_tasks)} tasks. Processing...")
        results = await asyncio.gather(*parsing_tasks, return_exceptions=True)

        bus_services = []
        current_bus_num = 1
        for idx, res in enumerate(results):
            if isinstance(res, TNSTCBusService):
                res.bus_number = current_bus_num
                bus_services.append(res)
                current_bus_num += 1
            elif isinstance(res, Exception):
                logger.error(f"Bus {idx}: Final parsing error: {res}")

        logger.info(
            f"OllamaParser: Completed. {len(bus_services)} parsed successfully."
        )
        return bus_services
