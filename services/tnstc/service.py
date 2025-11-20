import os
import re
from typing import Any, Dict, List, Optional
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

    async def _fetch_place_info(
        self, place_name: str, is_from_place: bool
    ) -> TNSTCPlaceInfo:
        """
        Resolves internal TNSTC Place IDs/Codes from a string name.
        Uses DB-backed cache, falling back to TNSTC API.
        """

        place_type = "From" if is_from_place else "To"
        logger.info(f"TNSTC: {place_type} Place lookup for: '{place_name}'")

        # 1. Check Cache
        try:
            cached = await get_place_from_cache("TNSTC", place_name)
        except Exception as e:
            cached = None
            logger.warning(f"Place cache lookup failed for '{place_name}': {e}")

        if cached:
            logger.debug(
                f"TNSTC Cache Hit: {place_name} -> "
                f"ID={cached['place_id']}, Code={cached['place_code']}"
            )

            return TNSTCPlaceInfo(
                id=cached["place_id"],
                code=cached["place_code"],
                name=cached["place_name"],
            )

        # 2. Fetch from API
        async with httpx.AsyncClient() as client:
            action = "LoadFromPlaceList" if is_from_place else "LoadTOPlaceList"
            match_param = "matchStartPlace" if is_from_place else "matchEndPlace"

            data = {"hiddenAction": action, match_param: place_name}

            logger.debug(
                f"TNSTC API Place Lookup [{place_type}]: '{place_name}' "
                f"with payload={data}"
            )

            try:
                response = await client.post(self.base_url, data=data, timeout=15.0)
                response.raise_for_status()
            except Exception as e:
                logger.error(f"TNSTC API Error during place lookup '{place_name}': {e}")
                raise RuntimeError("Failed to connect to TNSTC") from e

            raw_response = response.text.strip()
            logger.debug(
                f"TNSTC raw place lookup response length={len(raw_response)} "
                f"for '{place_name}'"
            )

            if not raw_response:
                logger.error(f"TNSTC returned empty place response for '{place_name}'")
                raise ValueError(f"Place not found: {place_name}")

            # TNSTC returns data separated by '^', format typically: ID:CODE:NAME
            place_list = [item for item in raw_response.split("^") if item]

            if not place_list:
                logger.error(
                    f"TNSTC: Could not find exact place match for: {place_name}. "
                    f"Raw response: '{raw_response}'"
                )

                raise ValueError(f"Could not find exact place match for: {place_name}.")

            first_match = place_list[0]
            parts = first_match.split(":")

            if len(parts) < 3:
                logger.error(
                    f"TNSTC: Invalid place format for '{place_name}': '{first_match}'"
                )

                raise ValueError(
                    f"External API returned invalid place format: {first_match}"
                )

            place = TNSTCPlaceInfo(id=parts[0], code=parts[1], name=parts[2])

            logger.info(
                f"TNSTC: Resolved '{place_name}' -> ID={place.id}, Code={place.code}"
            )

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

                logger.debug(f"TNSTC: Cached place '{place_name}' successfully.")
            except Exception as e:
                logger.warning(
                    f"Failed to cache place '{place_name}' "
                    f"(ID={place.id}, Code={place.code}): {e}"
                )

            return place

    def _filter_bus_services(
        self, bus_list: List[TNSTCBusService], request: TNSTCSearchRequest
    ) -> List[TNSTCBusService]:
        """
        Applies filtering logic (Price, Time, Bus Type) to parsed results.
        """

        logger.info(
            "TNSTC: Starting filtering for bus services. "
            f"Total before filter: {len(bus_list)}"
        )

        filtered_services: List[TNSTCBusService] = []

        # Defaults
        min_dep_str = request.min_departure_time or "00:00"
        max_dep_str = request.max_departure_time or "23:59"
        min_price = (
            request.min_price_in_rs if request.min_price_in_rs is not None else 0.0
        )

        max_price = (
            request.max_price_in_rs
            if request.max_price_in_rs is not None
            else float("inf")
        )

        logger.debug(
            "TNSTC: Filter criteria -> "
            f"Price: [{min_price}, {max_price}], "
            f"Departure: [{min_dep_str}, {max_dep_str}], "
            f"Allowed types: {request.allowed_bus_types}"
        )

        # Time conversion for comparison (HH:MM -> HHMM int)
        min_dep_int = int(min_dep_str.replace(":", ""))
        max_dep_int = int(max_dep_str.replace(":", ""))

        allowed_types_lower = (
            {t.lower() for t in request.allowed_bus_types}
            if request.allowed_bus_types
            else None
        )

        for service in bus_list:
            try:
                # 1. Price Filter
                if not (min_price <= service.price_in_rs <= max_price):
                    logger.debug(
                        f"Filtering out {service.trip_code}: price {service.price_in_rs} "
                        f"outside range [{min_price}, {max_price}]"
                    )

                    continue

                # 2. Time Validation
                if not re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", service.departure_time):
                    logger.warning(
                        f"Skipping service {service.trip_code} with invalid "
                        f"departure time format: {service.departure_time}"
                    )

                    continue

                dep_time_int = int(service.departure_time.replace(":", ""))
                if not (min_dep_int <= dep_time_int <= max_dep_int):
                    logger.debug(
                        f"Filtering out {service.trip_code}: departure "
                        f"{service.departure_time} outside range "
                        f"[{min_dep_str}, {max_dep_str}]"
                    )

                    continue

                # 3. Bus Type Filter
                if (
                    allowed_types_lower
                    and service.bus_type.lower() not in allowed_types_lower
                ):
                    logger.debug(
                        f"Filtering out {service.trip_code}: bus_type "
                        f"'{service.bus_type}' not in allowed {allowed_types_lower}"
                    )

                    continue

                filtered_services.append(service)

            except Exception as e:
                logger.warning(
                    f"Error filtering service {getattr(service, 'trip_code', 'UNKNOWN')}: {e}"
                )

                continue

        # Assign sequential bus numbers to filtered results
        for idx, service in enumerate(filtered_services, start=1):
            service.bus_number = idx

        logger.info(
            f"TNSTC: Filtering complete. {len(bus_list)} -> "
            f"{len(filtered_services)} services after filters."
        )
        return filtered_services

    def _pre_filter_buses(
        self, bus_metadata_list: List[Dict[str, Any]], request: TNSTCSearchRequest
    ) -> List[Dict[str, Any]]:
        """
        Pre-filter buses based on price, time, and bus type criteria.
        This happens BEFORE expensive parsing to reduce LLM costs.

        Args:
            bus_metadata_list: List of bus metadata dicts from extract_bus_metadata.
            request: The search request with filter criteria.

        Returns:
            Filtered list of bus metadata dicts.
        """
        min_price = request.min_price_in_rs if request.min_price_in_rs is not None else 0
        max_price = (
            request.max_price_in_rs
            if request.max_price_in_rs is not None
            else float("inf")
        )
        min_dep_time = request.min_departure_time or "00:00"
        max_dep_time = request.max_departure_time or "23:59"
        allowed_types = (
            {t.lower() for t in request.allowed_bus_types}
            if request.allowed_bus_types
            else None
        )

        # Convert times to int for comparison
        min_dep_int = int(min_dep_time.replace(":", ""))
        max_dep_int = int(max_dep_time.replace(":", ""))

        filtered = []
        filtered_out_count = 0

        for metadata in bus_metadata_list:
            # Price filter
            if not (min_price <= metadata["price_in_rs"] <= max_price):
                filtered_out_count += 1
                logger.debug(
                    f"Pre-filter: Bus {metadata['idx']} excluded by price: "
                    f"{metadata['price_in_rs']} not in [{min_price}, {max_price}]"
                )
                continue

            # Time filter
            dep_time = metadata["departure_time"]
            if re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", dep_time):
                dep_int = int(dep_time.replace(":", ""))
                if not (min_dep_int <= dep_int <= max_dep_int):
                    filtered_out_count += 1
                    logger.debug(
                        f"Pre-filter: Bus {metadata['idx']} excluded by time: "
                        f"{dep_time} not in [{min_dep_time}, {max_dep_time}]"
                    )
                    continue

            # Bus type filter
            if allowed_types and metadata["bus_type"].lower() not in allowed_types:
                filtered_out_count += 1
                logger.debug(
                    f"Pre-filter: Bus {metadata['idx']} excluded by bus_type: "
                    f"'{metadata['bus_type']}' not in {allowed_types}"
                )
                continue

            filtered.append(metadata)

        logger.info(
            f"TNSTC Pre-filter: {len(bus_metadata_list)} buses → "
            f"{len(filtered)} passed filters ({filtered_out_count} excluded)"
        )

        return filtered

    async def search_services(
        self, request: TNSTCSearchRequest, limit: Optional[int] = None
    ) -> List[TNSTCBusService]:
        """
        Orchestrates the full search flow with smart pre-filtering:
        Place Resolution -> HTTP Request -> Pre-filtering -> Limited Parsing -> Validation.

        Args:
            request: The search request with origin, destination, date, and filter criteria.
            limit: Optional maximum number of buses to parse (reduces LLM costs).
                   Applied AFTER pre-filtering based on price/time/bus_type.

        Returns:
            List of TNSTCBusService objects that match all criteria.
        """

        logger.info(
            "TNSTC: Starting search request. "
            f"{request.from_place_name} -> {request.to_place_name} "
            f"on {request.onward_date}"
        )

        try:
            # 1. Resolve Places
            from_place = await self._fetch_place_info(
                request.from_place_name, is_from_place=True
            )

            to_place = await self._fetch_place_info(
                request.to_place_name, is_from_place=False
            )

            logger.debug(
                "TNSTC: Resolved places -> "
                f"From(ID={from_place.id}, Code={from_place.code}), "
                f"To(ID={to_place.id}, Code={to_place.code})"
            )

            # 2. Construct Payload
            payload = {
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

            logger.debug(
                f"TNSTC: Constructed search payload for date={request.onward_date} "
                f"(return={request.return_date})"
            )

            async with httpx.AsyncClient(timeout=60.0) as client:
                logger.info("TNSTC: Sending Search Payload to TNSTC endpoint...")
                final_url = f"{self.base_url}?hiddenAction=SearchService"

                response = await client.post(final_url, data=payload)
                response.raise_for_status()
                logger.debug(
                    f"TNSTC: Search response status={response.status_code}, "
                    f"length={len(response.text)}"
                )

                # STEP 1: Always use BeautifulSoupParser for bus extraction
                bs_parser = BeautifulSoupParser()
                parser: AbstractBusParser = get_parser()

                try:
                    logger.info(
                        "TNSTC: Extracting bus HTMLs with BeautifulSoupParser..."
                    )
                    bus_html_list = bs_parser.extract_bus_htmls(response.text)
                    logger.info(
                        f"TNSTC: BeautifulSoupParser extracted {len(bus_html_list)} buses"
                    )

                    if not bus_html_list:
                        logger.warning(
                            "TNSTC: No buses found by BeautifulSoupParser"
                        )
                        return []

                    # STEP 2: Extract metadata for smart pre-filtering
                    logger.info("TNSTC: Extracting metadata for smart pre-filtering...")
                    bus_metadata_list = []
                    for idx, bus_html in enumerate(bus_html_list):
                        metadata = bs_parser.extract_bus_metadata(bus_html, idx)
                        if metadata:
                            bus_metadata_list.append(metadata)

                    # STEP 3: Pre-filter based on criteria (BEFORE expensive parsing)
                    filtered_metadata = self._pre_filter_buses(
                        bus_metadata_list, request
                    )

                    if not filtered_metadata:
                        logger.warning(
                            "TNSTC: No buses passed pre-filtering criteria"
                        )
                        return []

                    # STEP 4: Apply limit to reduce LLM costs
                    if limit and len(filtered_metadata) > limit:
                        logger.info(
                            f"TNSTC: Applying limit {limit} to {len(filtered_metadata)} pre-filtered buses"
                        )
                        filtered_metadata = filtered_metadata[:limit]

                    # STEP 5: Extract just the HTML for parsing
                    filtered_bus_htmls = [m["html"] for m in filtered_metadata]
                    logger.info(
                        f"TNSTC: Parsing {len(filtered_bus_htmls)} buses "
                        f"(after pre-filtering + limit)"
                    )

                    # STEP 6: Determine parsing strategy
                    if isinstance(parser, BeautifulSoupParser):
                        # Use BS results directly
                        logger.info("TNSTC: Using BeautifulSoupParser for full parsing")
                        raw_services = await bs_parser.parse_buses(
                            client, filtered_bus_htmls
                        )
                    else:
                        # LLM strategy: pass pre-filtered bus HTMLs
                        logger.info(
                            f"TNSTC: Passing {len(filtered_bus_htmls)} pre-filtered buses to "
                            f"{parser.__class__.__name__}"
                        )
                        raw_services = await parser.parse_buses(client, filtered_bus_htmls)

                except Exception as e:
                    # STEP 3: Fallback - let LLM parse everything
                    logger.warning(
                        f"TNSTC: BeautifulSoupParser pre-filtering failed: {e}. "
                        f"Falling back to full HTML parsing with {parser.__class__.__name__}"
                    )

                    if isinstance(parser, BeautifulSoupParser):
                        # BS already failed, return empty
                        logger.error(
                            "TNSTC: BeautifulSoupParser failed and no LLM parser available for fallback"
                        )
                        return []
                    else:
                        # Use LLM to parse full HTML
                        logger.info(
                            f"TNSTC: Using {parser.__class__.__name__} to parse full HTML as fallback"
                        )
                        raw_services = await parser.parse(client, response.text)

                logger.info(f"TNSTC: Parser returned {len(raw_services)} raw services.")

            # 4. Filter Results
            final_services = self._filter_bus_services(raw_services, request)

            logger.info(
                f"TNSTC: Search Complete. Found {len(final_services)} valid services."
            )
            return final_services

        except Exception as e:
            logger.error(f"TNSTC Service Search Failed: {e}", exc_info=True)
            return []

    async def search_places(self, query: str) -> List[TNSTCPlaceInfo]:
        """
        Searches for places matching the query string.
        First checks the local cache, then (optionally) could query the API if needed.
        For now, we rely on the cache and what we've learned from previous lookups.
        """
        logger.info(f"TNSTC: Searching for places matching '{query}'")

        # 1. Search in Cache
        cached_results = await search_places_in_cache("TNSTC", query)

        places = []
        for res in cached_results:
            places.append(
                TNSTCPlaceInfo(
                    id=res["place_id"],
                    code=res["place_code"],
                    name=res["place_name"],
                )
            )

        logger.info(f"TNSTC: Found {len(places)} matches in cache for '{query}'")
        return places
