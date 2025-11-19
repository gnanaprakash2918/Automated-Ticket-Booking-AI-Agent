import httpx
import re
import logging
from abc import ABC, abstractmethod
from typing import List, Optional, Type, TypeVar
from pydantic import BaseModel

from tnstc_api.config import TNSTC_DETAILS_URL
from tnstc_api.schemas import BusService
from utils.logging_setup import setup_logging

setup_logging()
log = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

class AbstractBusParser(ABC):
    async def _call_load_trip_details(self, client: httpx.AsyncClient, onclick_attr: str, bus_index: int) -> str:
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
            log.error(f"Network error calling loadTripDetails for bus {bus_index}: {e}")
            return ""

    @abstractmethod
    async def parse(self, client: httpx.AsyncClient, html_content: str, limit: Optional[int] = None) -> List[BusService]:
        raise NotImplementedError

class LLMInterface(ABC):
    @abstractmethod
    async def generate(self, prompt: str, **kwargs) -> str:
        raise NotImplementedError

    @abstractmethod
    async def generate_structured(self, schema: Type[T], prompt: str, system_prompt: str = "", **kwargs) -> T:
        raise NotImplementedError