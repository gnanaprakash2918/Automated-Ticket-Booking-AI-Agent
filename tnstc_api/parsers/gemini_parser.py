import re
import httpx
from typing import List, Optional
import logging
from tenacity import wait_exponential, stop_after_attempt, Retrying
import asyncio
from bs4 import BeautifulSoup
from pydantic import ValidationError

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, SystemMessage

from utils.helpers import minify_html

from .prompt_builder import PromptGenerator
from .base import AbstractBusParser 

from ..schemas import BusService, BusServiceWithReasoning
from ..config import GEMINI_API_KEY, GEMINI_MODEL, GEMINI_LOAD_TIMEOUT

from utils.logging_setup import setup_logging

setup_logging()
log = logging.getLogger(__name__)

class GeminiParser(AbstractBusParser):
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
                request_timeout=GEMINI_LOAD_TIMEOUT,
                temperature=0.0
            )

            self.prompt_gen = PromptGenerator()

            self.structured_llm = self.llm.with_structured_output(BusServiceWithReasoning)
        except ImportError:
            log.error("LangChain Google GENAI library not found. Please install 'langchain-google-genai'")
            raise
        except Exception as e:
            log.error(f"Failed to initialize Gemini LLM: {e}")
            raise
        
        self.system_prompt = self.prompt_gen.build_system_prompt(BusService)
        self.few_shot_examples = self.prompt_gen._build_few_shot_examples()
        self.total_chars_sent = 0
        self.total_requests = 0
            
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
        You are an expert parsing engine. You will receive two CLEANED HTML fragments: 
        1. MAIN_LIST_HTML (Primary data)
        2. DETAIL_TABLE_HTML (Secondary data)

        TASK:
        Extract specific fields defined in the JSON_SCHEMA.
        Merge data from both sources based on the "Source of Truth" hierarchy below.
        
        {self.few_shot_examples}

        ---
        NEW TASK
        ---
        MAIN_LIST_HTML
        {main_list_html}
        ---
        DETAIL_TABLE_HTML
        {detail_table_html}
        ---

        ### SOURCE OF TRUTH HIERARCHY (CRITICAL RULES)

        1. **PRIMARY IDENTIFIERS (Codes & Dynamic Info):**
           - **Source of Truth:** MAIN_LIST_HTML
           - **Fallback:** DETAIL_TABLE_HTML
           - Fields: `trip_code` (Service Code), `route_code` (Route No), `price_in_rs`, `seats_available`, `departure_time`, `arrival_time`, `duration`, `via_route`, `bus_type`.
           - *CRITICAL for Trip Code:* Always prefer the text inside the `<a>` tag in MAIN_LIST over the Detail Table, as the popup content can sometimes be mismatched.

        2. **SECONDARY STATIC DETAILS:**
           - **Source of Truth:** DETAIL_TABLE_HTML
           - **Fallback:** MAIN_LIST_HTML
           - Fields: `total_kms`, `operator` (Corporation), `child_fare`.

        ### FIELD SPECIFIC EXTRACTION LOGIC

        * `via_route`: Look for text starting with "Via-" in MAIN_LIST. Split by comma. 
            - Example: "Via-KARUR , DINDIGUL" -> ["KARUR", "DINDIGUL"]
            - Example: "Via-HOSUR" -> ["HOSUR"]
            - If not found, return `null` (not "NA").
        
        * `child_fare`: Strictly from DETAIL_TABLE_HTML "Child Fare" column.
        
        * `duration`: Extract the numeric value string. "6.10Hrs" -> "6.10". "5:30" -> "5.30".
        
        * `price_in_rs`: Extract integers only. Remove "Rs" or symbols.

        ### FAILURE HANDLING
        * If a value is missing in Primary AND Fallback sources, return "NA".
        * Exceptions: `via_route` returns `null`, `price_in_rs` returns `0` if missing.

        ### OUTPUT FORMAT
        Output strictly raw JSON. No markdown, no conversational text.
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
                message_size = sum(len(m.content) for m in messages)
                self.total_chars_sent += message_size
                self.total_requests += 1

                log.info(f"LLM_Parser Bus {bus_index} (Attempt {attempt.retry_state.attempt_number}): Sending Cleaned HTML ({message_size} chars) to LLM.") 
                
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
                    log.error(f"LLM_Parser Bus {bus_index}: Pydantic validation failed. Error: {e}", exc_info=True)
                    raise
                except Exception as e:
                    log.error(f"GeminiParser: Bus {bus_index}: Failed during LangChain invocation: {e}")
                    raise

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

        # 1. Fetch detailed HTML for all buses SEQUENTIALLY to avoid server state race conditions
        all_details_html = []
        log.info(f"GeminiParser: Starting sequential detail fetch for {len(bus_divs)} buses...")
        
        for idx, bus_div in enumerate(bus_divs):
            a_tag = bus_div.find("a", attrs={"data-target": "#TripcodePopUp", "onclick": True})
            onclick_attr = a_tag.get("onclick", "") if a_tag else ""

            if onclick_attr:
                # Await each request individually
                detail_html = await self._call_load_trip_details(client, str(onclick_attr), idx)
                all_details_html.append(detail_html)
            else:
                log.warning(f"GeminiParser Bus {idx}: No 'onclick' attribute found. Cannot fetch details.")
                all_details_html.append("")

        # 2. Create tasks to parse each bus using the two HTML sources
        parsing_tasks = []
        for idx, bus_div in enumerate(bus_divs):
            main_list_html = re.sub(r"[\r\n]+", "", str(bus_div))
            detail_table_html = re.sub(r"[\r\n]+", "", str(all_details_html[idx]))
            
            main_list_html = minify_html(str(bus_div))
            detail_table_html = minify_html(all_details_html[idx])
            
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

        avg_chars = self.total_chars_sent / max(self.total_requests, 1)

        log.info(f"GeminiParser: Successfully parsed {len(bus_services)} / {len(bus_divs)} bus services. "
                 f"Summary: {self.total_requests} requests, {self.total_chars_sent} total chars sent, "
                 f"avg {avg_chars:.0f} chars/request.")
        
        return bus_services