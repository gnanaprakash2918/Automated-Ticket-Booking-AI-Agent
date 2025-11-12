import httpx
from typing import List
import json
import logging
from pydantic import ValidationError
from tenacity import retry, wait_exponential, stop_after_attempt, RetryError

from ..schemas import BusService, BusServiceList, get_gemini_schema_for
from ..config import GEMINI_API_KEY, GEMINI_API_URL

log = logging.getLogger(__name__)

class GeminiParser:
    """
    Implements the BusParser interface using the Gemini API's structured output (JSON mode).
    
    This parser is resilient to HTML/CSS changes as it relies on an LLM
    to understand the content and structure it according to a provided schema.
    """

    def __init__(self):
        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY environment variable is not set. Cannot use GeminiParser.")
        
        self.api_url = f"{GEMINI_API_URL}?key={GEMINI_API_KEY}"
        
        self.service_list_schema = get_gemini_schema_for(BusServiceList)
        
        self.system_prompt = f"""
        You are an expert, automated HTML parsing engine. Your sole task is to extract all
        bus service details from the provided HTML content and return them as a
        single, valid JSON object.

        - You MUST adhere *strictly* to the provided JSON schema.
        - Extract *all* bus services listed in the HTML.
        - If a value is not found (e.g., 'via_route'), omit the field or set it to null.
        - Pay close attention to data types (e.g., 'price_in_rs' must be an integer).
        - Do NOT include any text, explanations, or markdown fences (```json) in your response.
        - Your entire response must be *only* the JSON object.
        """

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5),
        reraise=True
    )
    async def _call_gemini_api(
        self, 
        client: httpx.AsyncClient, 
        payload: dict
    ) -> BusServiceList:
        """
        Calls the Gemini API with exponential backoff.
        """
        log.debug("Calling Gemini API...")
        
        response = await client.post(self.api_url, json=payload, timeout=120.0)        
        response.raise_for_status() 
        
        result = response.json()

        try:
            candidate = result.get('candidates', [])[0]
            json_text = candidate['content']['parts'][0]['text']
            
            data = json.loads(json_text)            
            return BusServiceList(**data)
        
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            log.error(f"GeminiParser: Failed to parse valid JSON from LLM response. Error: {e}. Response: {result}")
            raise ValueError(f"Failed to parse LLM JSON response: {e}")
        except ValidationError as e:
            log.error(f"GeminiParser: LLM output failed Pydantic validation. Error: {e}. Data: {data}")
            raise
            

    async def parse(
        self, 
        client: httpx.AsyncClient, 
        html_content: str
    ) -> List[BusService]:
        """
        Parses the main HTML by sending the *entire* page to the
        Gemini API and asking for a structured JSON list in return.
        """
        log.info(f"Using GeminiParser to parse bus results...")
        
        user_prompt = f"""
        Please extract all bus service details from the following HTML content.
        Return the data as a JSON object matching the system prompt's schema.

        HTML_CONTENT:
        {html_content}
        """

        payload = {
            "contents": [{"parts": [{"text": user_prompt}]}],
            "systemInstruction": {"parts": [{"text": self.system_prompt}]},
            "generationConfig": {
                "responseMimeType": "application/json",
                "responseSchema": self.service_list_schema
            }
        }
        
        try:
            service_list = await self._call_gemini_api(client, payload)
            
            log.info(f"GeminiParser: Successfully parsed {len(service_list.services)} bus services.")
            return service_list.services
        
        except RetryError as e:
            log.error(f"GeminiParser: All retries failed. Could not get response from Gemini API. Last error: {e}")
            return []
        except httpx.HTTPStatusError as e:
            log.error(f"GeminiParser: Gemini API returned non-2xx status: {e.response.status_code}. Response: {e.response.text}")
            return []
        except Exception as e:
            log.error(f"GeminiParser: An unexpected error occurred: {e}", exc_info=True)
            return []