import os
import re
from typing import Any, Dict, List, Optional, Tuple
import httpx
from loguru import logger
from database.db import (
    MigrationManager,
    get_place_from_cache,
    save_place_to_cache,
    search_places_in_cache,
)
from ..base.service import BaseService
from .config import TNSTC_BASE_URL
from .parsers import get_parser
from .parsers.base import AbstractBusParser
from .parsers.bs_parser import BeautifulSoupParser
from .schemas import TNSTCBusService, TNSTCPlaceInfo, TNSTCSearchRequest


class AmbiguousPlaceError(Exception):
    """Raised when multiple places match the search query."""

    def __init__(self, candidates: List[TNSTCPlaceInfo]):
        self.candidates = candidates
        super().__init__(f"Found {len(candidates)} matching places. Please refine your search.")


class TNSTCService(BaseService):
    """
    Concrete implementation of the Transport Service for TNSTC.
    """

    def __init__(self) -> None:
        super().__init__("TNSTC")
        self.base_url: str = TNSTC_BASE_URL
        logger.info(f"TNSTCService initialized with base_url={self.base_url}")

    async def initialize_db(self):
        """
        Initialize DB using SQL-file based migrations via MigrationManager.
        """
        current_dir = os.path.dirname(os.path.abspath(__file__))
        migration_dir = os.path.join(current_dir, "db", "migrations")

        logger.info(f"Initializing TNSTC Database from: {migration_dir}")

        try:
            manager = MigrationManager(
                service_name="tnstc",
                migrations_dir=migration_dir,
            )

            await manager.migrate()
            logger.info("TNSTC Database migrations completed successfully.")
        except Exception as e:
            logger.critical(f"Database Initialization Failed: {e}")
            # Do not raise here to allow the app to start, but caching will fail

    async def _query_tnstc_places(self, query: str) -> List[TNSTCPlaceInfo]:
        """
        Helper to query TNSTC API for places and parse the response.
        """
        async with httpx.AsyncClient() as client:
            # 'matchStartPlace' works for both start and end place searches in this context
            data = {"hiddenAction": "LoadFromPlaceList", "matchStartPlace": query}
            
            try:
                response = await client.post(self.base_url, data=data, timeout=15.0)
                response.raise_for_status()
            except Exception as e:
                logger.error(f"TNSTC API Error during place lookup '{query}': {e}")
                raise RuntimeError("Failed to connect to TNSTC") from e

            raw_response = response.text.strip()
            if not raw_response:
                return []

            # TNSTC returns data separated by '^', format typically: ID:CODE:NAME
            place_strings = [item for item in raw_response.split("^") if item]
            candidates = []
            
            for p_str in place_strings:
                parts = p_str.split(":")
                if len(parts) >= 3:
                    candidates.append(
                        TNSTCPlaceInfo(id=parts[0], code=parts[1], name=parts[2])
                    )
            
            return candidates

    async def _fetch_place_info(
        self, place_name: str, is_from_place: bool
    ) -> TNSTCPlaceInfo:
        """
        Resolves internal TNSTC Place IDs/Codes from a string name.
        Uses DB-backed cache, falling back to TNSTC API.
        Raises AmbiguousPlaceError if multiple matches are found.
        """
        place_type = "From" if is_from_place else "To"
        logger.info(f"TNSTC: {place_type} Place lookup for: '{place_name}'")

        # 1. Check Cache
        try:
            cached = await get_place_from_cache("TNSTC", place_name)
            if cached:
                logger.debug(f"TNSTC Cache Hit: {place_name}")
                return TNSTCPlaceInfo(
                    id=cached["place_id"],
                    code=cached["place_code"],
                    name=cached["place_name"],
                )
        except Exception as e:
            logger.warning(f"Place cache lookup failed for '{place_name}': {e}")

        # 2. Fetch from API
        candidates = await self._query_tnstc_places(place_name)

        if not candidates:
            logger.error(f"TNSTC: Could not find place match for: {place_name}")
            raise ValueError(f"Place not found: {place_name}")

        # If multiple candidates found, raise AmbiguousPlaceError
        if len(candidates) > 1:
            logger.warning(
                f"TNSTC: Ambiguous place '{place_name}'. Found {len(candidates)} matches."
            )
            raise AmbiguousPlaceError(candidates)

        place = candidates[0]
        logger.info(f"TNSTC: Resolved '{place_name}' -> ID={place.id}, Code={place.code}")

        # 3. Save to Cache
        try:
            await save_place_to_cache(
                "TNSTC",
                {
                    "place_name": place.name,
                    "place_code": place.code,
                    "place_id": place.id,
                },
            )
        except Exception as e:
            logger.warning(f"Failed to cache place '{place_name}': {e}")

        return place

    def _construct_search_payload(
        self, from_place: TNSTCPlaceInfo, to_place: TNSTCPlaceInfo, request: TNSTCSearchRequest
    ) -> Dict[str, Any]:
        """Constructs the form data payload for the TNSTC search request."""
        return {
            "hiddenStartPlaceID": from_place.id,
            "hiddenEndPlaceID": to_place.id,
            "txtStartPlaceCode": from_place.code,
            "txtEndPlaceCode": to_place.code,
            "hiddenStartPlaceName": from_place.name,
            "hiddenEndPlaceName": to_place.name,
            "matchStartPlace": from_place.name,
            "matchEndPlace": to_place.name,
            "selectStartPlace": from_place.code,
            "selectEndPlace": to_place.code,
            "txtJourneyDate": request.onward_date,
            "txtReturnDate": request.return_date or "",
            "hiddenOnwardJourneyDate": request.onward_date,
            "hiddenReturnJourneyDate": request.return_date or "",
            "hiddenAction": "SearchService",
            "languageType": "E",
            "checkSingleLady": "N",
        }

    def _filter_bus_services(
        self, bus_list: List[TNSTCBusService], request: TNSTCSearchRequest
    ) -> List[TNSTCBusService]:
        """Applies filtering logic (Price, Time, Bus Type) to parsed results."""
        logger.info(f"TNSTC: Filtering {len(bus_list)} services.")

        filtered_services: List[TNSTCBusService] = []
        
        min_dep_str = request.min_departure_time or "00:00"
        max_dep_str = request.max_departure_time or "23:59"
        min_price = request.min_price_in_rs if request.min_price_in_rs is not None else 0.0
        max_price = request.max_price_in_rs if request.max_price_in_rs is not None else float("inf")

        min_dep_int = int(min_dep_str.replace(":", ""))
        max_dep_int = int(max_dep_str.replace(":", ""))

        allowed_types_lower = (
            {t.lower() for t in request.allowed_bus_types}
            if request.allowed_bus_types
            else None
        )

        for service in bus_list:
            try:
                # Price Filter
                if not (min_price <= service.price_in_rs <= max_price):
                    continue

                # Time Validation
                if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", service.departure_time):
                    continue

                dep_time_int = int(service.departure_time.replace(":", ""))
                if not (min_dep_int <= dep_time_int <= max_dep_int):
                    continue

                # Bus Type Filter
                if allowed_types_lower and service.bus_type.lower() not in allowed_types_lower:
                    continue

                filtered_services.append(service)

            except Exception as e:
                logger.warning(f"Error filtering service {getattr(service, 'trip_code', 'UNKNOWN')}: {e}")
                continue

        # Assign sequential bus numbers
        for idx, service in enumerate(filtered_services, start=1):
            service.bus_number = idx

        logger.info(f"TNSTC: Filtering complete. {len(filtered_services)} services remain.")
        return filtered_services

    def _pre_filter_buses(
        self, bus_metadata_list: List[Dict[str, Any]], request: TNSTCSearchRequest
    ) -> List[Dict[str, Any]]:
        """Pre-filter buses based on price, time, and bus type criteria."""
        min_price = request.min_price_in_rs if request.min_price_in_rs is not None else 0
        max_price = request.max_price_in_rs if request.max_price_in_rs is not None else float("inf")
        min_dep_time = request.min_departure_time or "00:00"
        max_dep_time = request.max_departure_time or "23:59"
        
        allowed_types = (
            {t.lower() for t in request.allowed_bus_types}
            if request.allowed_bus_types
            else None
        )

        min_dep_int = int(min_dep_time.replace(":", ""))
        max_dep_int = int(max_dep_time.replace(":", ""))

        filtered = []

        for metadata in bus_metadata_list:
            # Price filter
            if not (min_price <= metadata["price_in_rs"] <= max_price):
                continue

            # Time filter
            dep_time = metadata["departure_time"]
            if re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", dep_time):
                dep_int = int(dep_time.replace(":", ""))
                if not (min_dep_int <= dep_int <= max_dep_int):
                    continue

            # Bus type filter
            if allowed_types and metadata["bus_type"].lower() not in allowed_types:
                continue

            filtered.append(metadata)

        return filtered

    async def search_services(
        self,
        request: TNSTCSearchRequest,
        limit: Optional[int] = None,
        from_place: Optional[TNSTCPlaceInfo] = None,
        to_place: Optional[TNSTCPlaceInfo] = None,
    ) -> Tuple[List[TNSTCBusService], TNSTCPlaceInfo, TNSTCPlaceInfo]:
        """
        Orchestrates the full search flow with smart pre-filtering.
        """
        logger.info(
            f"TNSTC: Starting search request. {request.from_place_name} -> {request.to_place_name} on {request.onward_date}"
        )

        try:
            # 1. Resolve Places
            # Check if specific ID/Code provided in request to bypass lookup
            if not from_place:
                if request.from_place_id and request.from_place_code:
                    from_place = TNSTCPlaceInfo(
                        id=request.from_place_id,
                        code=request.from_place_code,
                        name=request.from_place_name
                    )
                    logger.info("TNSTC: Using provided From Place ID/Code.")
                else:
                    from_place = await self._fetch_place_info(request.from_place_name, is_from_place=True)
            
            if not to_place:
                if request.to_place_id and request.to_place_code:
                    to_place = TNSTCPlaceInfo(
                        id=request.to_place_id,
                        code=request.to_place_code,
                        name=request.to_place_name
                    )
                    logger.info("TNSTC: Using provided To Place ID/Code.")
                else:
                    to_place = await self._fetch_place_info(request.to_place_name, is_from_place=False)

            # 2. Construct Payload
            payload = self._construct_search_payload(from_place, to_place, request)

            # 3. Fetch and Parse
            async with httpx.AsyncClient(timeout=60.0) as client:
                logger.info("TNSTC: Sending Search Payload...")
                final_url = f"{self.base_url}?hiddenAction=SearchService"
                
                response = await client.post(final_url, data=payload)
                response.raise_for_status()
                
                # Parse Logic
                bs_parser = BeautifulSoupParser()
                parser: AbstractBusParser = get_parser()
                
                try:
                    bus_html_list = bs_parser.extract_bus_htmls(response.text)
                    if not bus_html_list:
                        logger.warning("TNSTC: No buses found by BeautifulSoupParser")
                        return [], from_place, to_place

                    # Pre-filter
                    bus_metadata_list = []
                    for idx, bus_html in enumerate(bus_html_list):
                        metadata = bs_parser.extract_bus_metadata(bus_html, idx)
                        if metadata:
                            bus_metadata_list.append(metadata)
                    
                    filtered_metadata = self._pre_filter_buses(bus_metadata_list, request)
                    
                    if limit and len(filtered_metadata) > limit:
                        filtered_metadata = filtered_metadata[:limit]
                    
                    filtered_bus_htmls = [m["html"] for m in filtered_metadata]
                    
                    if isinstance(parser, BeautifulSoupParser):
                        raw_services = await bs_parser.parse_buses(client, filtered_bus_htmls)
                    else:
                        raw_services = await parser.parse_buses(client, filtered_bus_htmls)

                except Exception as e:
                    logger.warning(f"TNSTC: Pre-filtering failed: {e}. Fallback to full parsing.")
                    if isinstance(parser, BeautifulSoupParser):
                        return [], from_place, to_place
                    raw_services = await parser.parse(client, response.text)

            # 4. Final Filter
            final_services = self._filter_bus_services(raw_services, request)
            return final_services, from_place, to_place

        except AmbiguousPlaceError:
            raise
        except Exception as e:
            logger.error(f"TNSTC Service Search Failed: {e}", exc_info=True)
            return [], from_place if 'from_place' in locals() else None, to_place if 'to_place' in locals() else None

    async def search_places(self, query: str) -> List[TNSTCPlaceInfo]:
        """
        Searches for places matching the query string.
        """
        logger.info(f"TNSTC: Searching for places matching '{query}'")

        # 1. Search in Cache
        cached_results = await search_places_in_cache("TNSTC", query)
        places = [
            TNSTCPlaceInfo(
                id=res["place_id"],
                code=res["place_code"],
                name=res["place_name"],
            )
            for res in cached_results
        ]

        # 2. If insufficient results, query API
        if len(places) < 5:
            logger.info("TNSTC: Insufficient cache results, querying API...")
            try:
                api_places = await self._query_tnstc_places(query)
                for place in api_places:
                    if not any(p.code == place.code for p in places):
                        places.append(place)
                        # Cache it
                        await save_place_to_cache(
                            "TNSTC",
                            {
                                "place_name": place.name,
                                "place_code": place.code,
                                "place_id": place.id,
                            },
                        )
            except Exception as e:
                logger.warning(f"TNSTC: API place search failed: {e}")

        return places
