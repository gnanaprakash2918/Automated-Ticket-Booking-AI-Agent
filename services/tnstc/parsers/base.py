from abc import ABC, abstractmethod
import logging
import re
from typing import List, Optional
import httpx
from utils.logger import setup_logging
from ..config import TNSTC_DETAILS_URL
from ..schemas import TNSTCBusService


setup_logging()
log = logging.getLogger(__name__)


class AbstractBusParser(ABC):
    """
    Defines the standard interface and implements shared utilities for a bus results parser.
    All concrete parser classes must inherit from this class and implement the 'parse' method.
    """

    async def _call_load_trip_details(
        self, client: httpx.AsyncClient, onclick_attr: str, bus_index: int
    ) -> str:
        """
        Extracts arguments and calls the LoadTripDetails endpoint (Shared Logic).

        This is a concrete implementation shared by all subclasses.
        """
        args = re.findall(r"'([^']*)'", str(onclick_attr))
        if len(args) < 6:
            log.error(f"Failed to parse onclick_attr: {onclick_attr}")
            return ""

        data = {
            "ServiceID": args[0],
            "TripCode": args[1],
            "StartPlaceID": args[2],
            "EndPlaceID": args[3],
            "JourneyDate": args[4],
            "ClassID": args[5],
        }

        try:
            response = await client.post(TNSTC_DETAILS_URL, data=data)
            response.raise_for_status()
            return response.text
        except httpx.RequestError as e:
            log.error(f"Network error calling loadTripDetails for bus {bus_index}: {e}")
            return ""

    @abstractmethod
    async def parse(
        self, client: httpx.AsyncClient, html_content: str, limit: Optional[int] = None
    ) -> List[TNSTCBusService]:
        """
        Parses the raw HTML of the bus search results page.

        Args:
            client: An httpx.AsyncClient for making any necessary sub-requests
                    (e.g., to get trip details).
            html_content: The raw HTML string of the main search results page.
            limit: If provided, stop parsing after this many buses
                   to prevent excess sub-requests.

        Returns:
            A list of TNSTCBusService objects.
        Subclasses must provide a concrete implementation for this method.
        """
        raise NotImplementedError
