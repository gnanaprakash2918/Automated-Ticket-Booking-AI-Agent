import httpx
from typing import List, Protocol, Optional
from ..schemas import BusService

class BusParser(Protocol):
    """
    Defines the standard interface for a bus results parser.
    
    Any class that implements this protocol must provide an async `parse` method
    that takes the main HTML content and an httpx client (for sub-requests)
    and returns a list of BusService objects.
    """

    async def parse(
        self, 
        client: httpx.AsyncClient, 
        html_content: str,
        limit: Optional[int] = None
    ) -> List[BusService]:
        """
        Parses the raw HTML of the bus search results page.

        Args:
            client: An httpx.AsyncClient for making any necessary sub-requests
                    (e.g., to get trip details).
            html_content: The raw HTML string of the main search results page.
            limit: If provided, stop parsing after this many buses
                   to prevent excess sub-requests.

        Returns:
            A list of BusService objects.
        """
        ...