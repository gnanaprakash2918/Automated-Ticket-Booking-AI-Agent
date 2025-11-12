import httpx
from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup, Tag
from ..schemas import BusService
import re
import asyncio
import logging
from ..config import TNSTC_DETAILS_URL

log = logging.getLogger(__name__)

class BeautifulSoupParser:
    """
    Implements the BusParser interface using BeautifulSoup for high-speed,
    selector-based HTML parsing.    
    """
    
    async def parse(
        self, 
        client: httpx.AsyncClient, 
        html_content: str
    ) -> List[BusService]:
        """
        Parses the raw HTML search results into a structured list of BusService models.

        It first tries to get detailed data by calling 'loadTripDetails' for each bus
        concurrently.
        """
        soup = BeautifulSoup(html_content, 'lxml')
        bus_services: List[BusService] = []
            
        detail_tasks = []
        temp_data_list = []
        bus_divs = soup.find_all('div', class_ = 'bus-list')

        # Scrape main list and create detail-call tasks
        for idx, bus_div in enumerate(bus_divs):
            try:
                # 1. Get data ONLY available in the main list 'bus_div'
                bus_type = str(bus_div.get('data-bus-type', 'N/A')).strip()
                seats_available = self._parse_seats(bus_div)
                via_route_list = self._parse_via_route(bus_div)
                
                # 1.4 Onclick attribute - Load Trip Details
                a_tag = bus_div.find("a", attrs={"data-target": "#TripcodePopUp", "onclick": True})
                onclick_attr = a_tag.get("onclick", "") if a_tag else ""

                # 2. Add task to get detailed HTML
                if onclick_attr:
                    detail_tasks.append(self._call_load_trip_details(client, str(onclick_attr)))
                else:
                    detail_tasks.append(asyncio.sleep(0, result="")) 

                temp_data_list.append({
                    "bus_type": bus_type,
                    "seats_available": seats_available,
                    "via_route_list": via_route_list
                })
                
            except Exception as e:
                log.error(f"Critical error in bs_parser (Pass 1) for bus {idx}: {e}")
                detail_tasks.append(asyncio.sleep(0, result=""))
                temp_data_list.append(None)

        # 3. Run all detail tasks in parallel
        all_details_html = await asyncio.gather(*detail_tasks)

        # 4. Combine main list data with detail data using the new hybrid logic
        for idx, details_html in enumerate(all_details_html):
            main_list_data = temp_data_list[idx]
            bus_div = bus_divs[idx]

            if main_list_data is None:
                continue
                
            try:
                parsed_details = self._parse_details_from_trip_html(details_html)
                fallback_data = self._parse_details_from_bus_div(bus_div)

                # 3. Create the final service_data, starting with fallback as base
                service_data = {
                    'operator': fallback_data.get('operator', 'N/A'),
                    'trip_code': fallback_data.get('trip_code', 'N/A'),
                    'route_code': fallback_data.get('route_code', 'N/A'),
                    'departure_time': fallback_data.get('departure_time', 'N/A'),
                    'arrival_time': fallback_data.get('arrival_time', 'N/A'),
                    'duration': fallback_data.get('duration', 'N/A'),
                    'price_in_rs': fallback_data.get('price_in_rs', 0)
                }
                
                total_kms = None
                child_fare = None

                # 4. Selectively overwrite with data from parsed_details
                if parsed_details:
                    service_data.update({k: v for k, v in parsed_details.items() if v})
                    
                    try:
                        price_str = parsed_details.get('price_in_rs_str')
                        if price_str:
                            service_data['price_in_rs'] = int(price_str)
                    except (ValueError, TypeError):
                        pass
                    
                    total_kms = parsed_details.get('total_kms')
                    child_fare = parsed_details.get('child_fare', "NA")

                # 5. Append the final merged object
                bus_services.append(BusService(
                    operator=service_data['operator'],
                    bus_type=main_list_data['bus_type'],
                    trip_code=service_data['trip_code'],
                    route_code=service_data['route_code'],
                    departure_time=service_data['departure_time'],
                    arrival_time=service_data['arrival_time'],
                    duration=service_data['duration'],
                    price_in_rs=service_data['price_in_rs'],
                    seats_available=main_list_data['seats_available'],
                    via_route=main_list_data['via_route_list'],
                    total_kms=total_kms,
                    child_fare=child_fare
                ))

            except Exception as e:
                log.error(f"Critical error in bs_parser (Pass 2) for bus {idx}: {e}")
                continue

        return bus_services

    # Helpers

    def _parse_seats(self, bus_div: Tag) -> int:
        """Extracts available seats from the bus_div."""
        seats_available = 0
        seats_text_element_candidates = bus_div.find_all('span', class_ = 'text-1')
        seats_text_element = next((s for s in seats_text_element_candidates if isinstance(s.string, str) and 'Seats Available' in s.string), None)
        
        if seats_text_element and seats_text_element.text is not None:
            try:
                seats_available = int(seats_text_element.text.split(' ')[0])
            except ValueError:
                log.warning('Could not convert the number of seats to an integer.')
        return seats_available

    def _parse_via_route(self, bus_div: Tag) -> Optional[List[str]]:
        """Extracts the 'via' route list from the bus_div."""
        via_route_list: Optional[List[str]] = None
        via_tag_candidates = [tag for tag in bus_div.find_all('small') if tag.get('style') and 'color: blue' in tag['style']]
        via_tag = via_tag_candidates[0] if via_tag_candidates else None
        
        if via_tag and via_tag.find('b'):
            via_b_tag = via_tag.find('b')
            if via_b_tag and via_b_tag.text is not None:
                via_text = via_b_tag.text.strip()
                if 'Via-' in via_text:
                    route_string = via_text.replace('Via-', '').strip()
                    if route_string: 
                        via_route_list = [stop.strip() for stop in route_string.split(',') if stop.strip()]
        return via_route_list

    async def _call_load_trip_details(self, client: httpx.AsyncClient, onclick_attr: str) -> str:
        """Extracts arguments and calls the LoadTripDetails endpoint."""
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
            log.error(f"Network error calling loadTripDetails: {e}")
            return ""

    def _parse_details_from_trip_html(self, trip_html: str) -> Optional[Dict[str, Any]]:
        """Helper to parse the detailed HTML from _call_load_trip_details."""
        if not trip_html:
            return None
        try:
            details_soup = BeautifulSoup(trip_html, 'lxml')
            data: Dict[str, Any] = {}
            
            rows = details_soup.find_all('tr')
            details_map = self._parse_key_value_table(rows)
            
            data['operator'] = details_map.get("Corporation")
            data['trip_code'] = details_map.get("Service Code")
            data['route_code'] = details_map.get("Route No.")
            data['total_kms'] = details_map.get("Total Kms")
            data['duration'] = details_map.get("Journey Hours")
            
            self._parse_fares(details_soup, data)
            self._parse_stops_table(details_soup, data)
            
            return data
        except Exception as e:
            log.error(f"Error parsing trip detail HTML: {e}")
            return None

    def _parse_details_from_bus_div(self, bus_div: Tag) -> dict:
        """Fallback helper to scrape data from the main list div."""
        data = {}
        
        op_el = bus_div.find('span', class_ = 'operator-name')
        data['operator'] = op_el.text.strip() if op_el else "N/A"
        
        time_divs = bus_div.find_all('div', class_='time-info')

        # Departure time
        if len(time_divs) > 0:
            span = time_divs[0].find('span')
            data['departure_time'] = span.get_text(strip=True) if span else "N/A"
        else:
            data['departure_time'] = "N/A"

        # Arrival time
        if len(time_divs) > 2:
            span = time_divs[2].find('span')
            data['arrival_time'] = span.get_text(strip=True) if span else "N/A"
        else:
            data['arrival_time'] = "N/A"

        dur_el = bus_div.find('span', class_='duration')
        data['duration'] = dur_el.text.strip().replace('Hrs', '').strip() if dur_el and dur_el.text else "N/A"
        
        price = 0
        price_div = bus_div.find('div', class_ = 'price')
        if price_div and price_div.contents:
            full_text = " ".join(str(el) for el in price_div.contents)
            tokens = full_text.split()
            try:
                amount = next(t for t in tokens if t.isdigit())
                price = int(amount)
            except (StopIteration, ValueError):
                log.warning("BS_Parser: Could not find numeric price in fallback.")
        data['price_in_rs'] = price
        
        code_span = next((s for s in bus_div.find_all('span', class_ = 'text-1 text-muted d-block') if s.text and '/' in s.text), None)
        if code_span:
            parts = code_span.text.strip().split('/', 1)
            data['trip_code'] = parts[0].strip()
            data['route_code'] = parts[1].strip() if len(parts) > 1 else "N/A"
        else:
            data['trip_code'], data['route_code'] = "N/A", "N/A"
            
        return data

    def _parse_key_value_table(self, rows: list) -> Dict[str, str]:
        """Parses <tr> elements into a key-value map."""
        details_map = {}
        for row in rows:
            label_cell = row.find('td', attrs={"class": "bodytextWithSecondMainColor"})
            value_cell = row.find('td', attrs={"class": "bodytextWithThirdMainColor"})
            if label_cell and value_cell:
                label = label_cell.text.replace(':', '').replace('\xa0', ' ').replace('*', '').strip()
                value = (value_cell.find('strong') or value_cell).text.strip()
                details_map[label] = value
        return details_map

    def _parse_fares(self, details_soup: BeautifulSoup, data: Dict[str, Any]) -> None:
        """Finds the Adult and Child fares."""
        data['price_in_rs_str'] = self._find_fare_value(details_soup, r"Adult\s*Fare")
        data['child_fare'] = self._find_fare_value(details_soup, r"Child\s*Fare")

    def _find_fare_value(self, details_soup: BeautifulSoup, pattern_str: str) -> Optional[str]:
        """Nested helper to find a specific fare by its label pattern."""
        try:
            fare_pattern = re.compile(pattern_str, re.IGNORECASE)
            fare_label = details_soup.find('strong', string=fare_pattern) or details_soup.find('div', string=fare_pattern) # type: ignore
            if not fare_label: return None

            parent_div = fare_label.find_parent('div')
            if not parent_div: return None
            price_cell = parent_div.find_next_sibling('td')
            if not price_cell: return None
            
            price_span = price_cell.find('span', class_='button')
            if price_span:
                return price_span.text.strip()
        except AttributeError:
            log.warning(f"AttributeError while parsing fare: {pattern_str}")
        return None

    def _parse_stops_table(self, details_soup: BeautifulSoup, data: Dict[str, Any]) -> None:
        """Parses departure and arrival times from the stops table."""
        list_heading_tr = details_soup.find('tr', class_='listHeading')
        if not list_heading_tr: return

        valid_rows = [r for r in list_heading_tr.find_next_siblings('tr') if r.find('td')]
        if not valid_rows: return

        try:
            dep_cells = valid_rows[0].find_all('td')
            if len(dep_cells) >= 4: data['departure_time'] = dep_cells[3].text.strip()
            arr_cells = valid_rows[-1].find_all('td')
            if len(arr_cells) >= 4: data['arrival_time'] = arr_cells[3].text.strip()
        except IndexError:
            log.warning("IndexError while parsing stops table rows.")

# assert issubclass(BeautifulSoupParser, BusParser)