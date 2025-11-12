import httpx
from typing import List, Optional
from bs4 import BeautifulSoup
from pydantic import ValidationError
from ..schemas import BusService
import json
import asyncio
import logging
from ..config import OLLAMA_API_URL, OLLAMA_MODEL
from tenacity import retry, wait_exponential, stop_after_attempt

log = logging.getLogger(__name__)

class OllamaParser:
    """
    Implements the BusParser interface using a local LLM (via Ollama)
    to parse HTML content chunk by chunk.
    """

    def __init__(self):
        self.bus_schema = json.dumps(BusService.model_json_schema(), indent=2)
        
        self.system_prompt = f"""
        You are an expert HTML parsing assistant. Your task is to extract data from a given
        HTML chunk that represents a *single* bus service.

        You MUST respond ONLY with a single, valid JSON object that strictly adheres to the
        provided JSON schema. Do not include any other text, explanations, or markdown
        code fences (like ```json).

        JSON SCHEMA:
        {self.bus_schema}
        """

    def _build_user_prompt(self, html_chunk: str) -> str:
        """Builds the final user prompt with the HTML chunk."""
        return f"""
        Please extract all available data from the HTML chunk below, following the
        JSON schema provided in the system prompt.

        Pay close attention to field types and descriptions:
        - "operator": The name of the bus corporation (e.g., "SALEM").
        - "bus_type": The class of bus (e.g., "AC 3X2").
        - "departure_time" and "arrival_time": Must be in "HH:MM" 24-hour format.
        - "duration": The journey time, e.g., "7.45" or "7:30".
        - "price_in_rs": The *integer* price (e.g., 350).
        - "seats_available": The *integer* number of seats.
        - "via_route": A list of stop names, or null if not present.
        - If a value is not found, set it to a sensible default (like 0, "N/A", or null).

        HTML CHUNK:
        {html_chunk}
        """

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(3), # Retry 3 times
        reraise=True
    )
    async def _parse_chunk_with_ollama(
        self, 
        client: httpx.AsyncClient, 
        html_chunk: str,
        bus_index: int
    ) -> Optional[BusService]:
        """
        Sends a single HTML chunk to the Ollama API for parsing and validation.
        Includes retry logic.
        """
        log.debug(f"OllamaParser: Parsing chunk {bus_index}...")
        user_prompt = self._build_user_prompt(html_chunk)
        
        payload = {
            "model": OLLAMA_MODEL,
            "system": self.system_prompt,
            "prompt": user_prompt,
            "format": "json",
            "stream": False
        }
        
        try:
            response = await client.post(OLLAMA_API_URL, json=payload, timeout=60.0)
            response.raise_for_status()
            
            raw_json_string = response.json().get("response")
            
            if not raw_json_string:
                log.warning(f"OllamaParser: Bus {bus_index}: Received empty response.")
                return None
                
            parsed_data = json.loads(raw_json_string)
            
            bus_service = BusService(**parsed_data)
            return bus_service
            
        except httpx.HTTPStatusError as e:
            log.error(f"OllamaParser: Bus {bus_index}: API returned status {e.response.status_code}. {e.response.text}")
            raise
        except httpx.RequestError as e:
            log.error(f"OllamaParser: Bus {bus_index}: Network error calling Ollama: {e}")
            raise
        except json.JSONDecodeError as e:
            log.error(f"OllamaParser: Bus {bus_index}: Failed to decode JSON from LLM: {e}. Response was: {raw_json_string}")
            return None
        except ValidationError as e:
            log.error(f"OllamaParser: Bus {bus_index}: LLM output failed Pydantic validation: {e}. Data was: {parsed_data}")
            return None
        except Exception as e:
            log.error(f"OllamaParser: Bus {bus_index}: Unexpected error: {e}")
            return None

    async def parse(
        self, 
        client: httpx.AsyncClient, 
        html_content: str
    ) -> List[BusService]:
        """
        Parses the main HTML by splitting it into chunks (one per bus)
        and sending each chunk to the Ollama LLM for parsing in parallel.
        """
        
        log.info(f"Using OllamaParser with model {OLLAMA_MODEL}...")
        
        soup = BeautifulSoup(html_content, 'lxml')
        bus_divs = soup.find_all('div', class_ = 'bus-list')
        
        if not bus_divs:
            log.warning("OllamaParser: No 'div.bus-list' elements found in HTML.")
            return []

        tasks = []
        for idx, bus_div in enumerate(bus_divs):
            chunk_html = str(bus_div)
            tasks.append(self._parse_chunk_with_ollama(client, chunk_html, idx))
            
        results = await asyncio.gather(*tasks)
        
        bus_services: List[BusService] = [service for service in results if service is not None]
        
        log.info(f"OllamaParser: Successfully parsed {len(bus_services)} / {len(bus_divs)} bus services.")
        
        return bus_services