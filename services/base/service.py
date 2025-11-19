from abc import ABC, abstractmethod
from typing import Any

class BaseService(ABC):
    """
    Abstract Base Class for Transport Services (e.g., TNSTC, RedBus).
    """

    def __init__(self, service_name: str):
        self.service_name = service_name

    @abstractmethod
    async def search_services(self, request: Any) -> Any:
        """
        Search for available transport services.
        """

        raise NotImplementedError("Subclasses must implement search_services method.")
