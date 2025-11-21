import re
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup, Tag
import httpx
from loguru import logger
from ..schemas import TNSTCBusService
from .base import AbstractBusParser


class BeautifulSoupParser(AbstractBusParser):
    """
    Implements the BusParser interface using BeautifulSoup for high-speed, selector-based HTML parsing.
    """

    def extract_bus_htmls(self, html_content: str) -> List[str]:
        """
        Extract individual bus div HTML snippets for hybrid parsing strategy.
        """
        soup = BeautifulSoup(html_content, "lxml")
        bus_divs = soup.find_all("div", class_="bus-list")
        logger.info(
            f"BeautifulSoupParser.extract_bus_htmls: Found {len(bus_divs)} bus divs"
        )
        return [str(div) for div in bus_divs]

    def extract_bus_metadata(self, bus_html: str, idx: int) -> Optional[Dict[str, Any]]:
        """
        Extract basic metadata from bus HTML without expensive detail fetching.
        """
        soup = BeautifulSoup(bus_html, "lxml")
        bus_div = soup.find("div", class_="bus-list")

        if not bus_div:
            logger.warning(f"Bus {idx}: No bus-list div found in HTML snippet")
            return None

        # Extract price
        price = 0
        price_div = bus_div.find("div", class_="price")
        if price_div:
            match = re.search(r"(\d+)", price_div.get_text(strip=True))
            if match:
                price = int(match.group(1))

        # Extract departure time
        departure_time = "N/A"
        time_divs = bus_div.find_all("div", class_="time-info")
        if len(time_divs) > 0:
            span = time_divs[0].find("span")
            if span:
                departure_time = span.get_text(strip=True)

        # Extract bus type
        bus_type = str(bus_div.get("data-bus-type", "N/A")).strip()

        return {
            "idx": idx,
            "price_in_rs": price,
            "departure_time": departure_time,
            "bus_type": bus_type,
            "html": bus_html,
        }

    async def parse_buses(
        self,
        client: httpx.AsyncClient,
        bus_html_list: List[str],
        limit: Optional[int] = None,
    ) -> List[TNSTCBusService]:
        """
        Parse individual bus HTML snippets (hybrid parsing strategy).
        """
        logger.info(
            f"BeautifulSoupParser.parse_buses: Processing {len(bus_html_list)} "
            f"pre-filtered bus HTML snippets"
        )

        if limit is not None and len(bus_html_list) > limit:
            bus_html_list = bus_html_list[:limit]

        reconstructed_html = "\n".join(bus_html_list)
        return await self.parse(client, reconstructed_html, limit)

    async def parse(
        self, client: httpx.AsyncClient, html_content: str, limit: Optional[int] = None
    ) -> List[TNSTCBusService]:
        """
        Parses the raw HTML search results into a structured list of TNSTCBusService models.
        """
        soup = BeautifulSoup(html_content, "lxml")
        bus_services: List[TNSTCBusService] = []

        temp_data_list = []
        bus_divs = soup.find_all("div", class_="bus-list")

        if limit is not None and len(bus_divs) > limit:
            logger.info(
                f"BeautifulSoupParser: Limiting processing to first {limit} buses (found {len(bus_divs)})."
            )
            bus_divs = bus_divs[:limit]

        logger.info(
            f"BeautifulSoupParser: Starting hybrid parse. Processing {len(bus_divs)} bus elements."
        )

        # Scrape main list and prepare for sequential detail fetch
        for idx, bus_div in enumerate(bus_divs):
            try:
                # 1. Get data ONLY available in the main list 'bus_div'
                bus_type = str(bus_div.get("data-bus-type", "N/A")).strip()
                seats_available = self._parse_seats(bus_div)
                via_route_list = self._parse_via_route(bus_div)

                # 1.4 Onclick attribute - Load Trip Details
                a_tag = bus_div.find(
                    "a", attrs={"data-target": "#TripcodePopUp", "onclick": True}
                )
                onclick_attr = a_tag.get("onclick", "") if a_tag else ""

                temp_data_list.append(
                    {
                        "bus_type": bus_type,
                        "seats_available": seats_available,
                        "via_route_list": via_route_list,
                        "onclick_attr": onclick_attr,
                    }
                )

            except Exception as e:
                logger.error(f"Critical error in bs_parser (Pass 1) for bus {idx}: {e}")
                temp_data_list.append(None)

        # 3. Fetch detailed HTML SEQUENTIALLY
        all_details_html = []
        logger.info(
            f"BeautifulSoupParser: Starting sequential detail fetch for {len(temp_data_list)} buses..."
        )

        for idx, data_item in enumerate(temp_data_list):
            if data_item and data_item.get("onclick_attr"):
                detail_html = await self._call_load_trip_details(
                    client, str(data_item["onclick_attr"]), idx
                )
                all_details_html.append(detail_html)
            else:
                if data_item:
                    logger.warning(
                        f"BS_Parser Bus {idx}: No 'onclick' attribute found. Cannot fetch details."
                    )
                all_details_html.append("")

        # 4. Combine main list data with detail data
        for idx, details_html in enumerate(all_details_html):
            main_list_data = temp_data_list[idx]
            bus_div = bus_divs[idx]

            if main_list_data is None:
                continue

            try:
                parsed_details = self._parse_details_from_trip_html(details_html)
                fallback_data = self._parse_details_from_bus_div(
                    bus_div, main_list_data.get("onclick_attr")
                )

                # 3. Create the final service_data using Main List as the BASE
                service_data = {
                    "operator": fallback_data.get("operator", "N/A"),
                    "trip_code": fallback_data.get("trip_code", "N/A"),
                    "route_code": fallback_data.get("route_code", "N/A"),
                    "departure_time": fallback_data.get("departure_time", "N/A"),
                    "arrival_time": fallback_data.get("arrival_time", "N/A"),
                    "duration": fallback_data.get("duration", "N/A"),
                    "price_in_rs": fallback_data.get("price_in_rs", 0),
                }

                logger.debug(
                    f"BS_Parser Bus {idx}: Fallback Price: {fallback_data.get('price_in_rs')}, Trip Code: {fallback_data.get('trip_code')}"
                )

                total_kms = None
                child_fare = None

                # 4. Selectively merge details
                if parsed_details:
                    primary_sot_fields = [
                        "trip_code",
                        "route_code",
                        "departure_time",
                        "arrival_time",
                        "duration",
                        "price_in_rs",
                    ]

                    for k, v in parsed_details.items():
                        if not v:
                            continue
                        if k in primary_sot_fields:
                            current_val = service_data.get(k)
                            if current_val not in [None, "N/A", "", 0]:
                                continue

                        if k == "price_in_rs_str":
                            if service_data.get("price_in_rs", 0) == 0:
                                try:
                                    service_data["price_in_rs"] = int(v)
                                except (ValueError, TypeError):
                                    pass
                            continue

                        service_data[k] = v

                    total_kms = parsed_details.get("total_kms")
                    child_fare = parsed_details.get("child_fare", "NA")

                logger.info(
                    f"BS_Parser Bus {idx} MERGED: Operator: {service_data['operator']}, Trip Code: {service_data['trip_code']}"
                )

                # 5. Append with explicit bus_number
                bus_services.append(
                    TNSTCBusService(
                        bus_number=idx + 1,  # Explicit sequential numbering
                        operator=service_data["operator"],
                        bus_type=main_list_data["bus_type"],
                        trip_code=service_data["trip_code"],
                        route_code=service_data["route_code"],
                        departure_time=service_data["departure_time"],
                        arrival_time=service_data["arrival_time"],
                        duration=service_data["duration"],
                        price_in_rs=service_data["price_in_rs"],
                        seats_available=main_list_data["seats_available"],
                        via_route=main_list_data["via_route_list"],
                        total_kms=total_kms,
                        child_fare=child_fare,
                    )
                )

            except Exception as e:
                logger.error(f"Critical error in bs_parser (Pass 2) for bus {idx}: {e}")
                continue

        return bus_services

    # Helpers (same as before)
    def _parse_seats(self, bus_div: Tag) -> int:
        seats_available = 0
        seats_text_element_candidates = bus_div.find_all("span", class_="text-1")
        seats_text_element = next(
            (
                s
                for s in seats_text_element_candidates
                if isinstance(s.string, str) and "Seats Available" in s.string
            ),
            None,
        )

        if seats_text_element and seats_text_element.text is not None:
            try:
                seats_available = int(seats_text_element.text.split(" ")[0])
            except ValueError:
                logger.warning("Could not convert the number of seats to an integer.")
        return seats_available

    def _parse_via_route(self, bus_div: Tag) -> Optional[List[str]]:
        via_route_list: Optional[List[str]] = None
        via_tag_candidates = [
            tag
            for tag in bus_div.find_all("small")
            if (b_tag := tag.find("b")) and "Via-" in b_tag.get_text()
        ]

        if via_tag_candidates:
            via_tag = via_tag_candidates[0]
            via_b_tag = via_tag.find("b")
            if via_b_tag and via_b_tag.text is not None:
                via_text = via_b_tag.text.strip()
                if "Via-" in via_text:
                    route_string = via_text.replace("Via-", "").strip()
                    if route_string:
                        via_route_list = [
                            stop.strip()
                            for stop in route_string.split(",")
                            if stop.strip()
                        ]
        return via_route_list

    def _parse_details_from_trip_html(self, trip_html: str) -> Optional[Dict[str, Any]]:
        if not trip_html:
            return None
        try:
            details_soup = BeautifulSoup(trip_html, "lxml")
            data: Dict[str, Any] = {}

            rows = details_soup.find_all("tr")
            details_map = self._parse_key_value_table(rows)

            data["operator"] = details_map.get("Corporation")
            data["trip_code"] = details_map.get("Service Code")
            data["route_code"] = details_map.get("Route No.")
            data["total_kms"] = details_map.get("Total Kms")
            data["duration"] = details_map.get("Journey Hours")

            self._parse_fares(details_soup, data)
            self._parse_stops_table(details_soup, data)

            return data
        except Exception as e:
            logger.error(f"Error parsing trip detail HTML: {e}")
            return None

    def _parse_details_from_bus_div(self, bus_div: Tag, onclick_attr: str = "") -> dict:
        data = {}
        op_el = bus_div.find("span", class_="operator-name")
        data["operator"] = op_el.text.strip() if op_el else "N/A"

        time_divs = bus_div.find_all("div", class_="time-info")
        # Safely extract departure and arrival times: ensure the span exists before calling get_text
        if len(time_divs) > 0:
            dep_span = time_divs[0].find("span")
            data["departure_time"] = (
                dep_span.get_text(strip=True) if dep_span else "N/A"
            )
        else:
            data["departure_time"] = "N/A"

        if len(time_divs) > 2:
            arr_span = time_divs[2].find("span")
            data["arrival_time"] = arr_span.get_text(strip=True) if arr_span else "N/A"
        else:
            data["arrival_time"] = "N/A"

        dur_el = bus_div.find("span", class_="duration")
        data["duration"] = (
            dur_el.text.strip().replace("Hrs", "").strip()
            if dur_el and dur_el.text
            else "N/A"
        )

        price = 0
        price_div = bus_div.find("div", class_="price")
        if price_div:
            price_text = price_div.get_text(strip=True)
            match = re.search(r"(\d+)", price_text)
            if match:
                price = int(match.group(1))
        data["price_in_rs"] = price

        data["trip_code"], data["route_code"] = "N/A", "N/A"

        if onclick_attr:
            try:
                match = re.search(r"loadTripDetails\('([^']+)'", onclick_attr)
                if match:
                    args_list = match.group(1).split(",")
                    if len(args_list) >= 12:
                        data["trip_code"] = args_list[10].strip()
                        data["route_code"] = args_list[11].strip()
                        return data
            except Exception:
                pass

        code_span = next(
            (
                s
                for s in bus_div.find_all("span", class_="text-1")
                if s.text and "/" in s.text
            ),
            None,
        )

        if code_span:
            parts = list(code_span.stripped_strings)
            if len(parts) >= 3 and parts[1] == "/":
                data["trip_code"] = parts[0].strip()
                data["route_code"] = parts[2].strip()
            else:
                raw_parts = code_span.get_text().split("/", 1)
                data["trip_code"] = raw_parts[0].strip()
                data["route_code"] = (
                    raw_parts[1].strip() if len(raw_parts) > 1 else "N/A"
                )

        return data

    def _parse_key_value_table(self, rows: list) -> Dict[str, str]:
        details_map = {}
        for row in rows:
            label_cell = row.find("td", attrs={"class": "bodytextWithSecondMainColor"})
            value_cell = row.find("td", attrs={"class": "bodytextWithThirdMainColor"})
            if label_cell and value_cell:
                label = (
                    label_cell.text.replace(":", "")
                    .replace("\xa0", " ")
                    .replace("*", "")
                    .strip()
                )
                value = (value_cell.find("strong") or value_cell).text.strip()
                details_map[label] = value
        return details_map

    def _parse_fares(self, details_soup: BeautifulSoup, data: Dict[str, Any]) -> None:
        data["price_in_rs_str"] = self._find_fare_value(details_soup, r"Adult\s*Fare")
        data["child_fare"] = self._find_fare_value(details_soup, r"Child\s*Fare")

    def _find_fare_value(
        self, details_soup: BeautifulSoup, pattern_str: str
    ) -> Optional[str]:
        try:
            fare_pattern = re.compile(pattern_str, re.IGNORECASE)
            fare_label = details_soup.find(
                "strong",
                string=fare_pattern,  # type: ignore
            ) or details_soup.find("div", string=fare_pattern)  # type: ignore
            if not fare_label:
                return None

            label_cell = fare_label.find_parent("td")
            if not label_cell:
                return None

            price_cell = label_cell.find_next_sibling("td")
            if not price_cell:
                return None

            price_span = price_cell.find("span", class_="button")
            if price_span:
                return price_span.text.strip()
        except AttributeError:
            logger.warning(f"AttributeError while parsing fare: {pattern_str}")
        return None

    def _parse_stops_table(
        self, details_soup: BeautifulSoup, data: Dict[str, Any]
    ) -> None:
        list_heading_tr = details_soup.find("tr", class_="listHeading")
        if not list_heading_tr:
            return

        valid_rows = [
            r for r in list_heading_tr.find_next_siblings("tr") if r.find("td")
        ]
        if not valid_rows:
            return

        try:
            dep_cells = valid_rows[0].find_all("td")
            if len(dep_cells) >= 4:
                data["departure_time"] = dep_cells[3].text.strip()
            arr_cells = valid_rows[-1].find_all("td")
            if len(arr_cells) >= 4:
                data["arrival_time"] = arr_cells[3].text.strip()
        except IndexError:
            logger.warning("IndexError while parsing stops table rows.")
