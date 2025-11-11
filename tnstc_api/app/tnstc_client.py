import httpx
from fastapi import HTTPException, status
from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup
from schemas import PlaceInfo, BusService, SearchRequest
from dotenv import load_dotenv
import os
import re

load_dotenv()

BASE_URL = os.getenv('TNSTC_BASE_URL', 'https://www.tnstc.in/OTRSOnline/jqreq.do?')

# Get Place Information based on its Name
async def get_place_info(client: httpx.AsyncClient, place_name: str, is_from_place: bool) -> PlaceInfo:
    """
    Retrieves the internal ID and Code for a given place name.
    """

    action = "LoadFromPlaceList" if is_from_place else "LoadTOPlaceList"
    match_param = "matchStartPlace" if is_from_place else "matchEndPlace"

    data = {
        "hiddenAction": action,
        match_param: place_name,
    }

    try:
        response = await client.post(BASE_URL, data = data)
        response.raise_for_status()
    except httpx.RequestError as e:
        raise HTTPException(status_code = status.HTTP_503_SERVICE_UNAVAILABLE, detail = f"External API network error during place lookup: {e}")

    # Expected response format: "488:DHA:DHARMAPURI^"
    raw_response = response.text.strip()
    place_list = [item for item in raw_response.split('^') if item]
    
    if not place_list:
        raise HTTPException(status_code = status.HTTP_404_NOT_FOUND, 
                            detail = f"Could not find exact place match for: {place_name}. Try a broader search.")

    first_match = place_list[0]
    parts = first_match.split(':')
    
    if len(parts) < 3:
        raise HTTPException(status_code = status.HTTP_500_INTERNAL_SERVER_ERROR, 
                            detail = f"External API returned invalid place format: {first_match}")

    return PlaceInfo(id = parts[0], code = parts[1], name = parts[2])


# Parse the HTML to JSON
async def parse_bus_results(client: httpx.AsyncClient, html_content: str) -> List[BusService]:
    """
    Parses the raw HTML search results into a structured list of BusService models.

    It first tries to get detailed data by calling 'loadTripDetails' for each bus.
    If that fails, it falls back to scraping the data from the main list.
    """

    soup = BeautifulSoup(html_content, 'lxml')
    bus_services = []

    # Go through each bus in the bus list
    for idx, bus_div in enumerate(soup.find_all('div', class_ = 'bus-list')):
        try:
            # 1 Get data ONLY available in the main list 'bus_div'
            
            # 1.1 Bus Type
            bus_type_raw = bus_div.get('data-bus-type')
            bus_type = str(bus_type_raw).strip() if bus_type_raw is not None else "N/A"
            
            # 1.2 Seats Available
            seats_available = 0
            seats_text_element_candidates = bus_div.find_all('span', class_ = 'text-1')
            seats_text_element = next((s for s in seats_text_element_candidates if isinstance(s.string, str) and 'Seats Available' in s.string), None)
            
            if seats_text_element and seats_text_element.text is not None:
                try:
                    seats_available = int(seats_text_element.text.split(' ')[0])
                except ValueError:
                    print('Could not convert the number of seats to an integer.')

            # 1.3 Via Route
            via_route = None
            via_tag_candidates = [tag for tag in bus_div.find_all('small') if tag.get('style') and 'color: blue' in tag['style']]
            via_tag = via_tag_candidates[0] if via_tag_candidates else None
            
            if via_tag and via_tag.find('b'):
                via_b_tag = via_tag.find('b')
                if via_b_tag and via_b_tag.text is not None:
                    via_text = via_b_tag.text.strip()
                    if 'Via-' in via_text:
                        via_route = via_text.replace('Via-', '').strip()
            
            # 1.4 Onclick attribute - Load Trip Details
            a_tag = bus_div.find("a", attrs={"data-target": "#TripcodePopUp", "onclick": True})
            onclick_attr = a_tag.get("onclick", "") if a_tag else ""

            # 2 Tries to get data from detailed HMTL Page
            details_html = ""
            if onclick_attr:
                details_html = await call_load_trip_details(client, str(onclick_attr))

            parsed_details = _parse_details_from_trip_html(details_html)

            # Old
            operator_element = bus_div.find('span', class_ = 'operator-name')
            operator_name = operator_element.text.strip() if operator_element else "N/A"
            
            time_info_divs = bus_div.find_all('div', class_='time-info')

            # 2. Departure Time
            departure_time = "N/A"
            if len(time_info_divs) > 0 and time_info_divs[0]:
                departure_span = time_info_divs[0].find('span')
                if departure_span and departure_span.text is not None:
                    departure_time = departure_span.text.strip()
            
            # 3. Arrival Time
            arrival_time = "N/A"
            if len(time_info_divs) > 2 and time_info_divs[2]:
                arrival_span = time_info_divs[2].find('span')
                if arrival_span and arrival_span.text is not None:
                    arrival_time = arrival_span.text.strip()
            
            # 4. Duration 
            duration = "N/A"
            duration_element = bus_div.find('span', class_='duration')
            if duration_element and duration_element.text is not None:
                duration = duration_element.text.strip().replace('Hrs', '')
            
            # 5. Price Extraction
            price = 0
            price_div = bus_div.find('div', class_ = 'price')

            if price_div and price_div.contents:
                full_text = " ".join(str(element) for element in price_div.contents)
                tokens = full_text.split()

                try:
                    currency = next(t for t in tokens if t == "Rs")                    
                    amount = next(t for t in tokens if t.isdigit())
                    price = int(amount)
                except StopIteration:
                    print("Could not find 'Rs' or a numeric amount in the data.")
                except ValueError:
                    print('Could not convert the amount to an integer.')

            # 6. Trip/Route Code Extraction
            trip_code, route_code = "N/A", "N/A"
            code_span_parent_candidates = bus_div.find_all('span', class_ = 'text-1 text-muted d-block')
            code_span_parent = next((s for s in code_span_parent_candidates if s.text and '/' in s.text), None)
            
            if code_span_parent:
                codes_text = code_span_parent.text.strip() if code_span_parent.text is not None else "N/A / N/A"
                parts = codes_text.split('/', 1)
                trip_code = parts[0].strip()
                route_code = parts[1].strip() if len(parts) > 1 else "N/A"
            
            bus_services.append(BusService(
                operator=operator_name,
                bus_type=bus_type, 
                trip_code=trip_code,
                route_code=route_code,
                departure_time=departure_time,
                arrival_time=arrival_time,
                duration=duration,
                price_in_rs=price,
                seats_available=seats_available,
                via_route=via_route
            ))
        except Exception as e:
            continue

    return bus_services

async def call_load_trip_details(client: httpx.AsyncClient, onclick_attr: str) -> str:
    """
    Extracts arguments from the onclick attribute string for calling LoadTripDetails.
    """

    args = re.findall(r"'([^']*)'", str(onclick_attr))

    data = {
        "ServiceID": args[0],
        "TripCode": args[1],
        "StartPlaceID": args[2],
        "EndPlaceID": args[3],
        "JourneyDate": args[4],
        "ClassID": args[5],
    }

    URL = "https://www.tnstc.in/OTRSOnline/advanceNewBooking.do"

    try:
        response = await client.post(URL, data=data)
        response.raise_for_status()
        return response.text
    except httpx.RequestError as e:
        print(f"Network error calling loadTripDetails: {e}")
        return ""

def filter_bus_services(
    bus_list: List[BusService], 
    request: SearchRequest
) -> List[BusService]:
    """Applies price, time, and bus type filters to the parsed list of bus services."""
    
    filtered_services = []

    # Get filter values, using defaults if None (Pydantic will set defaults for missing fields)
    min_dep_str = request.min_departure_time if request.min_departure_time is not None else "00:00"
    max_dep_str = request.max_departure_time if request.max_departure_time is not None else "23:59"

    min_price = request.min_price_in_rs if request.min_price_in_rs is not None else 0
    max_price = request.max_price_in_rs if request.max_price_in_rs is not None else float('inf')
    
    min_dep_int = int(min_dep_str.replace(':', ''))
    max_dep_int = int(max_dep_str.replace(':', ''))
    
    # Bus types are generally case-insensitive in filtering
    allowed_types_lower = {t.lower() for t in request.allowed_bus_types} if request.allowed_bus_types else None

    for service in bus_list:
        try:
            # 1. Price Filter
            price_ok = (service.price_in_rs >= min_price) and \
                       (service.price_in_rs <= max_price)

            # 2. Time Filter
            if not re.fullmatch(r'([01]\d|2[0-3]):[0-5]\d', service.departure_time):
                continue 
                
            dep_time_int = int(service.departure_time.replace(':', ''))
            time_ok = (dep_time_int >= min_dep_int) and (dep_time_int <= max_dep_int)
            
            # 3. Bus Type Filter
            type_ok = True
            if allowed_types_lower is not None:

                # Check if the bus type (in lowercase) is in the allowed set
                type_ok = service.bus_type.lower() in allowed_types_lower

            if price_ok and time_ok and type_ok:
                filtered_services.append(service)
        except Exception:
            continue

    return filtered_services

# Helpers

def _parse_key_value_table(rows: list) -> Dict[str, str]:
    """
    Parses all <tr> elements from the main details table into a key-value map.
    """

    details_map = {}
    for row in rows:
        label_cell = row.find('td', attrs={"class": "bodytextWithSecondMainColor"})
        value_cell = row.find('td', attrs={"class": "bodytextWithThirdMainColor"})

        if label_cell and value_cell:
            label = label_cell.text.strip().replace(':', '').strip()
            strong_val = value_cell.find('strong')
            value = (strong_val.text.strip() if strong_val 
                     else value_cell.text.strip())
            details_map[label] = value
    return details_map

def _parse_fares(details_soup: BeautifulSoup, data: Dict[str, Any]) -> None:
    """
    Finds the Adult and Child fares.
    """
    
    def find_fare_value(pattern_str: str) -> Optional[str]:
        """Nested helper to find a specific fare by its label pattern."""
        try:
            fare_label = details_soup.find(
                'strong',
                string = lambda text: bool(text and re.search(pattern_str, text))
            )
            
            if not fare_label:
                return None

            price_span = (
                fare_label.find_parent('div')
                .find_next_sibling('td')
                .find('span', attrs={'class': 'button'})
            )
            
            if price_span:
                return price_span.text.strip()
        except AttributeError:
            return None
        return None

    data['price_in_rs_str'] = find_fare_value(r"Adult\s*Fare")
    data['child_fare'] = find_fare_value(r"Child\s*Fare")

def _parse_stops_table(details_soup: BeautifulSoup, data: Dict[str, Any]) -> None:
    """Parses the departure and arrival times from the stops table"""

    stops_table = details_soup.find('table', attrs={'id': 'table5'})
    if not stops_table:
        return

    dep_cell = stops_table.select_one('tr:nth-of-type(2) td:nth-of-type(4)')
    if dep_cell:
        data['departure_time'] = dep_cell.text.strip()
    
    arr_cell = stops_table.select_one('tr:nth-of-type(3) td:nth-of-type(4)')
    if arr_cell:
        data['arrival_time'] = arr_cell.text.strip()

def _parse_details_from_trip_html(trip_html: str) -> Optional[Dict[str, Any]]:
    """
    Helper to parse the detailed HTML from call_load_trip_details.
    Returns a dictionary with extracted data or None if parsing fails.
    """
    try:
        details_soup = BeautifulSoup(trip_html, 'lxml')
        data: Dict[str, Any] = {}
        
        rows = details_soup.find_all('tr')
        details_map = _parse_key_value_table(rows)
        
        data['operator'] = details_map.get("Corporation")
        data['trip_code'] = details_map.get("Service Code")
        data['route_code'] = details_map.get("Route No.")
        data['total_kms'] = details_map.get("Total Kms")
        data['duration'] = details_map.get("Journey Hours")
        
        _parse_fares(details_soup, data)
        
        _parse_stops_table(details_soup, data)
        
        essentials = [
            'operator', 'duration', 'trip_code', 'route_code', 
            'price_in_rs_str', 'departure_time', 'arrival_time'
        ]
        
        if all(data.get(k) for k in essentials):
            return data
        else:
            missing = [k for k in essentials if not data.get(k)]
            print(f"Warning: Missing essential data from trip detail HTML: {missing}")
            return None
    
    except Exception as e:
        print(f"Error parsing trip detail HTML: {e}")
        return None