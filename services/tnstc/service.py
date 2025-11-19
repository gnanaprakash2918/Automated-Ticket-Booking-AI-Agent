import httpx
import logging
import re
from typing import List
from async_lru import alru_cache

from base.service import BaseService
from .schemas import TNSTCPlaceInfo, TNSTCBusService, TNSTCSearchRequest
from .parsers import get_parser
from .parsers.base import AbstractBusParser
from .config import TNSTC_BASE_URL
from utils.logging_setup import setup_logging

setup_logging()
log = logging.getLogger(__name__)

class TNSTCService(BaseService):
    """
    Concrete implementation of the Transport Service for TNSTC.
    """

    def __init__(self) -> None:
        super().__init__("TNSTC")
        self.base_url: str = TNSTC_BASE_URL

    @staticmethod
    @alru_cache(maxsize=128)
    async def _fetch_place_info(place_name: str, is_from_place: bool) -> TNSTCPlaceInfo:
        """
        Resolves internal TNSTC Place IDs/Codes from a string name.
        Cached to reduce redundant network calls.
        """

        async with httpx.AsyncClient() as client:
            action = "LoadFromPlaceList" if is_from_place else "LoadTOPlaceList"
            match_param = "matchStartPlace" if is_from_place else "matchEndPlace"
            
            data = { "hiddenAction": action, match_param: place_name }
            
            place_type = "From" if is_from_place else "To"
            log.info(f"TNSTC: {place_type} Place lookup for: '{place_name}'") 

            try:
                response = await client.post(TNSTC_BASE_URL, data=data)
                response.raise_for_status()
            except httpx.RequestError as e:
                raise RuntimeError(f"External API network error during place lookup: {e}")

            raw_response = response.text.strip()
            
            # TNSTC returns data separated by '^'
            place_list = [item for item in raw_response.split('^') if item]
            
            if not place_list:
                raise ValueError(f"Could not find exact place match for: {place_name}.")

            # Format is usually: ID:CODE:NAME
            first_match = place_list[0]
            parts = first_match.split(':')
            
            if len(parts) < 3:
                raise ValueError(f"External API returned invalid place format: {first_match}")

            log.info(f"TNSTC: Resolved '{place_name}' -> ID={parts[0]}, Code={parts[1]}") 
            return TNSTCPlaceInfo(id=parts[0], code=parts[1], name=parts[2])

    def _filter_bus_services(self, bus_list: List[TNSTCBusService], request: TNSTCSearchRequest) -> List[TNSTCBusService]:
        """
        Applies filtering logic (Price, Time, Bus Type) to parsed results.
        """
        filtered_services: List[TNSTCBusService] = []

        # Defaults
        min_dep_str = request.min_departure_time or "00:00"
        max_dep_str = request.max_departure_time or "23:59"
        min_price = request.min_price_in_rs if request.min_price_in_rs is not None else 0.0
        max_price = request.max_price_in_rs if request.max_price_in_rs is not None else float('inf')
        
        # Time conversion for comparison (HH:MM -> HHMM int)
        min_dep_int = int(min_dep_str.replace(':', ''))
        max_dep_int = int(max_dep_str.replace(':', ''))
        
        allowed_types_lower = {t.lower() for t in request.allowed_bus_types} if request.allowed_bus_types else None
        
        for service in bus_list:
            try:
                # 1. Price Filter
                if not (min_price <= service.price_in_rs <= max_price):
                    continue

                # 2. Time Validation
                if not re.fullmatch(r'([01]\d|2[0-3]):[0-5]\d', service.departure_time):
                    log.warning(f"Skipping service with invalid departure time format: {service.departure_time}")
                    continue 
                    
                dep_time_int = int(service.departure_time.replace(':', ''))
                if not (min_dep_int <= dep_time_int <= max_dep_int):
                    continue
                
                # 3. Bus Type Filter
                if allowed_types_lower and service.bus_type.lower() not in allowed_types_lower:
                    continue

                filtered_services.append(service)

            except Exception as e:
                log.warning(f"Error filtering service {service.trip_code}: {e}")
                continue

        return filtered_services

    async def search_services(self, request: TNSTCSearchRequest) -> List[TNSTCBusService]:
        """
        Orchestrates the full search flow: Place Resolution -> HTTP Request -> Parsing -> Filtering.
        """
        log.info(f"TNSTC: Starting search request. {request.from_place_name} -> {request.to_place_name} on {request.onward_date}")

        try:
            # 1. Resolve Places
            from_place = await self._fetch_place_info(request.from_place_name, True)
            to_place = await self._fetch_place_info(request.to_place_name, False)


            # 2. Construct Payload
            payload = {
                'hiddenStartPlaceID': from_place.id,
                'hiddenEndPlaceID': to_place.id,
                'txtStartPlaceCode': from_place.code,
                'txtEndPlaceCode': to_place.code,
                'hiddenStartPlaceName': from_place.name,
                'hiddenEndPlaceName': to_place.name,
                'matchStartPlace': from_place.name,
                'matchEndPlace': to_place.name,
                'selectStartPlace': from_place.code,
                'selectEndPlace': to_place.code,
                'txtJourneyDate': request.onward_date,
                'txtReturnDate': request.return_date or "",
                'hiddenOnwardJourneyDate': request.onward_date,
                'hiddenReturnJourneyDate': request.return_date or "",
                'hiddenAction': 'SearchService',
                'languageType': 'E',
                'checkSingleLady': 'N'
            }

            async with httpx.AsyncClient(timeout=45.0) as client:
                log.info("TNSTC: Sending Search Payload...")
                
                final_url = f"{self.base_url}?hiddenAction=SearchService"
                response = await client.post(final_url, data=payload)
                response.raise_for_status()

                # 3. Parse Results
                parser: AbstractBusParser = get_parser()
                log.info(f"TNSTC: Parsing results using strategy: {parser.__class__.__name__}")
                
                # The parser expects the client to make sub-requests for details
                raw_services = await parser.parse(client, response.text)

            # 4. Filter Results
            final_services = self._filter_bus_services(raw_services, request)
            
            log.info(f"TNSTC: Search Complete. Found {len(final_services)} valid services.")
            return final_services

        except Exception as e:
            log.error(f"TNSTC Service Search Failed: {e}", exc_info=True)
            return []