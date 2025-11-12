import httpx
from fastapi import HTTPException, status
from typing import List, Optional
from .schemas import PlaceInfo, BusService, SearchRequest 
import re
import logging
from utils.logging_setup import setup_logging
from async_lru import alru_cache
from .config import TNSTC_BASE_URL

from .parsers import get_parser
from .parsers.base import BusParser

setup_logging()
log = logging.getLogger(__name__)


# Get Place Information

@alru_cache(maxsize=128)
async def get_place_info(client: httpx.AsyncClient, place_name: str, is_from_place: bool) -> PlaceInfo:
    """
    Retrieves the internal ID and Code for a given place name.
    Results are cached in memory.
    """
    action = "LoadFromPlaceList" if is_from_place else "LoadTOPlaceList"
    match_param = "matchStartPlace" if is_from_place else "matchEndPlace"
    data = { "hiddenAction": action, match_param: place_name }
    
    place_type = "From" if is_from_place else "To"
    log.info(f"Attempting {place_type} Place lookup for: '{place_name}'") 

    try:
        response = await client.post(TNSTC_BASE_URL, data = data)
        response.raise_for_status()
    except httpx.RequestError as e:
        raise HTTPException(status_code = status.HTTP_503_SERVICE_UNAVAILABLE, detail = f"External API network error during place lookup: {e}")

    raw_response = response.text.strip()
    place_list = [item for item in raw_response.split('^') if item]
    
    if not place_list:
        raise HTTPException(status_code = status.HTTP_404_NOT_FOUND, 
                            detail = f"Could not find exact place match for: {place_name}.")

    first_match = place_list[0]
    parts = first_match.split(':')
    
    if len(parts) < 3:
        raise HTTPException(status_code = status.HTTP_500_INTERNAL_SERVER_ERROR, 
                            detail = f"External API returned invalid place format: {first_match}")

    log.info(f"Place lookup SUCCESS for '{place_name}': ID={parts[0]}, Code={parts[1]}, Name='{parts[2]}'") 
    return PlaceInfo(id = parts[0], code = parts[1], name = parts[2])


# Parse Bus Results

async def parse_bus_results(
    client: httpx.AsyncClient, 
    html_content: str, 
    limit: Optional[int] = None
) -> List[BusService]:
    """
    Parses the raw HTML search results using the configured strategy.
    
    The 'limit' parameter is passed to the parser to stop processing
    early, preventing unnecessary sub-requests.
    """
    parser: BusParser = get_parser()
    
    log.info(f"Calling bus parser with strategy: {parser.__class__.__name__}. Limit: {limit if limit is not None else 'None'}.") 
    
    try:
        # Pass the limit down to the parser
        bus_services = await parser.parse(client, html_content, limit)
        return bus_services
    except Exception as e:
        log.error(f"Unhandled error during parsing strategy '{parser.__class__.__name__}': {e}", exc_info=True)
        return []

# Filter Bus Services

def filter_bus_services(
    bus_list: List[BusService], 
    request: SearchRequest
) -> List[BusService]:
    """Applies price, time, and bus type filters to the parsed list of bus services."""
    
    filtered_services = []

    min_dep_str = request.min_departure_time or "00:00"
    max_dep_str = request.max_departure_time or "23:59"
    min_price = request.min_price_in_rs if request.min_price_in_rs is not None else 0
    max_price = request.max_price_in_rs if request.max_price_in_rs is not None else float('inf')
    
    min_dep_int = int(min_dep_str.replace(':', ''))
    max_dep_int = int(max_dep_str.replace(':', ''))
    
    allowed_types_lower = {t.lower() for t in request.allowed_bus_types} if request.allowed_bus_types else None
    
    log.info(f"Applying filters: Price ({min_price}-{max_price}), Time ({min_dep_str}-{max_dep_str}), Types: {allowed_types_lower if allowed_types_lower else 'All'}") 

    for service in bus_list:
        try:
            price_ok = (service.price_in_rs >= min_price) and (service.price_in_rs <= max_price)

            if not re.fullmatch(r'([01]\d|2[0-3]):[0-5]\d', service.departure_time):
                log.warning(f"Skipping service with invalid departure time: {service.departure_time}")
                continue 
                
            dep_time_int = int(service.departure_time.replace(':', ''))
            time_ok = (dep_time_int >= min_dep_int) and (dep_time_int <= max_dep_int)
            
            type_ok = True
            if allowed_types_lower is not None:
                type_ok = service.bus_type.lower() in allowed_types_lower

            if price_ok and time_ok and type_ok:
                filtered_services.append(service)
            else:
                log.debug(f"Service {service.trip_code} filtered out: Price OK={price_ok}, Time OK={time_ok}, Type OK={type_ok}") 

        except Exception as e:
            log.warning(f"Error filtering service {service.trip_code}: {e}")
            continue

    return filtered_services