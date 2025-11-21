# Not Used for Now

from typing import List, Optional
from pydantic import BaseModel


class Place(BaseModel):
    """Generic place model."""

    name: str
    code: str
    id: Optional[str] = None


class TransportService(BaseModel):
    """Generic transport service model."""

    operator: str
    service_type: str
    departure_time: str
    arrival_time: str
    duration: str
    price: float
    currency: str = "INR"
    seats_available: int
    source: str
    destination: str


class SearchResult(BaseModel):
    """Generic search result."""

    source_place: Place
    destination_place: Place
    services: List[TransportService]
