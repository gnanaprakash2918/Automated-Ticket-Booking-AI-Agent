import asyncio
from typing import List, Optional
from bs4 import BeautifulSoup
import httpx
from loguru import logger
from tenacity import Retrying, stop_after_attempt, wait_exponential
from llm.interface import LLMInterface
from llm.ollama import OllamaLLM
from utils.helpers import minify_html
from ..config import OLLAMA_CONCURRENCY_LIMIT
from ..schemas import TNSTCBusService
from .base import AbstractBusParser


class OllamaParser(AbstractBusParser):
    """
    Implements the BusParser interface using the OllamaLLM adapter.
    Utilizes LLMInterface for prompt management and handles local concurrency limits.
    """

    def __init__(self):
        try:
            logger.info("Initializing OllamaParser with OllamaLLM...")
            self.llm: LLMInterface = OllamaLLM(prompt_dir="services/tnstc/prompts")

            self.system_prompt = self.llm.construct_system_prompt(
                schema=TNSTCBusService, filename="system_prompt.txt"
            )

            logger.debug(
                f"OllamaParser system prompt loaded (length={len(self.system_prompt)} chars)"
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
        """
        Parses a single bus.
        Uses LLMInterface.construct_user_prompt to merge HTML into the template.
        """

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

            logger.debug(
                f"Bus {bus_index}: User prompt constructed (length={len(user_prompt)} chars)"
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

                    logger.info(
                        f"Bus {bus_index} SUCCESS: "
                        f"{bus_service.operator} - {bus_service.trip_code}"
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
        """
        Wrapper to strictly limit concurrent Ollama requests using a Semaphore.
        Crucial for local LLM inference stability.
        """

        async with semaphore:
            logger.debug(
                f"OllamaParser: [SEMAPHORE ACQUIRED] Bus {idx}. "
                f"Active slots: {OLLAMA_CONCURRENCY_LIMIT - semaphore._value}"
            )

            try:
                result = await self._parse_bus_with_llm(
                    main_list_html, detail_table_html, idx
                )

                logger.debug(
                    f"OllamaParser: Bus {idx} parsing completed under semaphore."
                )

                return result
            finally:
                logger.debug(f"OllamaParser: [SEMAPHORE RELEASED] Bus {idx}.")

    async def parse(
        self, client: httpx.AsyncClient, html_content: str, limit: Optional[int] = None
    ) -> List[TNSTCBusService]:
        """
        Orchestration: Finds divs, fetches details, parses concurrently (throttled).
        """

        logger.info(
            f"Using OllamaParser (Interface Loader strategy). "
            f"Concurrency Limit: {OLLAMA_CONCURRENCY_LIMIT}"
        )

        semaphore = asyncio.Semaphore(OLLAMA_CONCURRENCY_LIMIT)

        soup = BeautifulSoup(html_content, "lxml")
        bus_divs = soup.find_all("div", class_="bus-list")

        logger.debug(
            f"OllamaParser: Found {len(bus_divs)} 'div.bus-list' elements in HTML."
        )

        if not bus_divs:
            logger.warning("OllamaParser: No 'div.bus-list' elements found.")
            return []

        all_details_html = []
        logger.info(f"OllamaParser: Fetching details for {len(bus_divs)} buses...")

        for idx, bus_div in enumerate(bus_divs):
            logger.debug(f"Bus {idx}: Extracting onclick attribute for trip details...")
            a_tag = bus_div.find(
                "a", attrs={"data-target": "#TripcodePopUp", "onclick": True}
            )

            onclick_attr = a_tag.get("onclick", "") if a_tag else ""

            if onclick_attr:
                logger.debug(
                    f"Bus {idx}: Found onclick attribute, calling details endpoint..."
                )

                detail_html = await self._call_load_trip_details(
                    client, str(onclick_attr), idx
                )

                logger.debug(
                    f"Bus {idx}: Details HTML loaded (length={len(detail_html)} chars)"
                )

                all_details_html.append(detail_html)
            else:
                logger.warning(f"Bus {idx}: No onclick attribute found.")
                all_details_html.append("")

        parsing_tasks = []
        for idx, bus_div in enumerate(bus_divs):
            main_clean = minify_html(str(bus_div))
            detail_clean = minify_html(all_details_html[idx])

            logger.debug(
                f"OllamaParser: Enqueuing bus {idx} for LLM parsing "
                f"(main_len={len(main_clean)}, detail_len={len(detail_clean)})"
            )

            parsing_tasks.append(
                self._wrapper_parse_chunk(semaphore, main_clean, detail_clean, idx)
            )

        logger.info(f"OllamaParser: Queued {len(parsing_tasks)} tasks. Processing...")
        results = await asyncio.gather(*parsing_tasks, return_exceptions=True)

        bus_services = []
        for idx, res in enumerate(results):
            if isinstance(res, TNSTCBusService):
                logger.debug(f"Bus {idx}: Parsed successfully into TNSTCBusService.")
                bus_services.append(res)
            elif isinstance(res, Exception):
                logger.error(f"Bus {idx}: Final parsing error: {res}")
            else:
                logger.error(
                    f"Bus {idx}: Unexpected result type from parsing: {type(res)}"
                )

        logger.info(
            f"OllamaParser: Completed. {len(bus_services)}/{len(bus_divs)} parsed successfully."
        )

        # Apply limit AFTER parsing all buses to avoid missing potential matches
        if limit is not None and len(bus_services) > limit:
            logger.info(f"OllamaParser: Applying limit of {limit} to {len(bus_services)} parsed buses.")
            bus_services = bus_services[:limit]

        return bus_services
