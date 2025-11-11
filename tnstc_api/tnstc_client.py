import httpx
from fastapi import HTTPException, status
from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup, Tag
from .schemas import PlaceInfo, BusService, SearchRequest 
from dotenv import load_dotenv
import os
import re
import asyncio
import logging
from utils.logging_setup import setup_logging

setup_logging()
log = logging.getLogger(__name__)

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

    It first tries to get detailed data by calling 'loadTripDetails' for each bus
    concurrently. It then intelligently merges this detailed data with fallback
    data scraped from the main list.
    """

    soup = BeautifulSoup(html_content, 'lxml')
    bus_services = []
        
    detail_tasks = []
    temp_data_list = []
    bus_divs = soup.find_all('div', class_ = 'bus-list')

    # Scrape main list and create detail-call tasks
    for idx, bus_div in enumerate(bus_divs):
        try:
            # 1. Get data ONLY available in the main list 'bus_div'
            
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
                    log.warning('Could not convert the number of seats to an integer.')

            # 1.3 Via Route
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
            
            # 1.4 Onclick attribute - Load Trip Details
            a_tag = bus_div.find("a", attrs={"data-target": "#TripcodePopUp", "onclick": True})
            onclick_attr = a_tag.get("onclick", "") if a_tag else ""

            # 2. Add task to get detailed HTML
            if onclick_attr:
                detail_tasks.append(call_load_trip_details(client, str(onclick_attr)))
            else:
                detail_tasks.append(asyncio.sleep(0, result="")) 

            temp_data_list.append({
                "bus_type": bus_type,
                "seats_available": seats_available,
                "via_route_list": via_route_list
            })
            
        except Exception as e:
            log.error(f"Critical error in parse_bus_results (Pass 1) for bus {idx}: {e}")
            detail_tasks.append(asyncio.sleep(0, result="")) # Add empty task
            temp_data_list.append(None) # Add placeholder

    # 3. Run all detail tasks in parallel
    all_details_html = await asyncio.gather(*detail_tasks)

    # 4. Combine main list data with detail data using the new hybrid logic
    for idx, details_html in enumerate(all_details_html):
        main_list_data = temp_data_list[idx]
        bus_div = bus_divs[idx]

        if main_list_data is None:
            # Skip if Pass 1 failed for this bus
            continue
            
        try:
            # 1. Get data from the new (preferred) source
            # This dict might be incomplete (e.g., {'operator': 'SALEM'})
            parsed_details = _parse_details_from_trip_html(details_html)

            # 2. Get data from the old (fallback) source
            # This dict will also have data (e.g., {'departure_time': '19:50'})
            fallback_data = _parse_details_from_bus_div(bus_div)

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
            
            # These are only available in the new parser
            total_kms = None
            child_fare = None

            # 4. Selectively overwrite with (better) data from parsed_details
            if parsed_details:
                # Overwrite any fields the new parser found
                if parsed_details.get('operator'):
                    service_data['operator'] = parsed_details['operator']
                if parsed_details.get('trip_code'):
                    service_data['trip_code'] = parsed_details['trip_code']
                if parsed_details.get('route_code'):
                    service_data['route_code'] = parsed_details['route_code']
                if parsed_details.get('departure_time'):
                    service_data['departure_time'] = parsed_details['departure_time']
                if parsed_details.get('arrival_time'):
                    service_data['arrival_time'] = parsed_details['arrival_time']
                if parsed_details.get('duration'):
                    service_data['duration'] = parsed_details['duration']
                
                try:
                    price_str = parsed_details.get('price_in_rs_str')
                    if price_str:
                        service_data['price_in_rs'] = int(price_str)
                except (ValueError, TypeError):
                    pass # Keep the fallback price if conversion fails
                
                # Get the extra data that only parsed_details has
                total_kms = parsed_details.get('total_kms')
                child_fare = parsed_details.get('child_fare')

                if child_fare is None:
                    child_fare = "NA"
            
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
            log.error(f"Critical error in parse_bus_results (Pass 2) for bus {idx}: {e}")
            continue

    return bus_services

async def call_load_trip_details(client: httpx.AsyncClient, onclick_attr: str) -> str:
    """
    Extracts arguments from the onclick attribute string for calling LoadTripDetails.
    """

    args = re.findall(r"'([^']*)'", str(onclick_attr))

    if len(args) < 6:
        log.error(f"Failed to parse onclick_attr: {onclick_attr}")
        return ""

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
        log.error(f"Network error calling loadTripDetails: {e}")
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
            # Check if departure time is a valid HH:MM format
            if not re.fullmatch(r'([01]\d|2[0-3]):[0-5]\d', service.departure_time):
                log.warning(f"Skipping service with invalid departure time: {service.departure_time}")
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
        except Exception as e:
            log.warning(f"Error filtering service {service.trip_code}: {e}")
            continue

    return filtered_services

# Helpers

def _parse_key_value_table(rows: list) -> Dict[str, str]:
    """
    Parses all <tr> elements from the main details table into a key-value map.
    Cleans the labels to remove ':', '*', and '\xa0'.
    """

    details_map = {}
    for row in rows:
        label_cell = row.find('td', attrs={"class": "bodytextWithSecondMainColor"})
        value_cell = row.find('td', attrs={"class": "bodytextWithThirdMainColor"})

        if label_cell and value_cell:
            # Clean up the label text
            label = label_cell.text.strip()
            label = label.replace(':', '')
            label = label.replace('\xa0', ' ')
            label = label.replace('*', '')
            label = label.strip()
            
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
            fare_pattern = re.compile(pattern_str, re.IGNORECASE) # Added IGNORECASE
            fare_label = details_soup.find(
                'strong',
                string = fare_pattern # type: ignore
            )
            
            if not fare_label:
                # Handle the case where the label might not be in a <strong> tag
                fare_div = details_soup.find('div', string=fare_pattern) # type: ignore
                if fare_div:
                    fare_label = fare_div
                else:
                    return None

            # Find the price span, which is in a complex structure
            # Traverse up to the common ancestor and then find the price
            
            # Try to find <td> -> <div> -> <strong>
            parent_div = fare_label.find_parent('div')
            if not parent_div:
                return None

            price_cell = parent_div.find_next_sibling('td')
            if not price_cell:
                return None
            
            price_span = price_cell.find('span', class_='button')
            
            if price_span:
                return price_span.text.strip()
                
        except AttributeError:
            log.warning(f"AttributeError while parsing fare: {pattern_str}")
            return None
        return None

    data['price_in_rs_str'] = find_fare_value(r"Adult\s*Fare")
    data['child_fare'] = find_fare_value(r"Child\s*Fare")

def _parse_stops_table(details_soup: BeautifulSoup, data: Dict[str, Any]) -> None:
    """
    Parses the departure and arrival times from the stops table
    by finding the 'listHeading' row and processing its siblings.
    """

    # Find the header row of the stops table
    list_heading_tr = details_soup.find('tr', class_='listHeading')
    if not list_heading_tr:
        log.warning("Could not find 'listHeading' row in trip details.")
        return

    data_rows = list_heading_tr.find_next_siblings('tr')   
    valid_rows = [r for r in data_rows if r.find('td')]
    
    if not valid_rows:
        log.warning("Found 'listHeading' but no data rows after it.")
        return

    try:
        dep_cells = valid_rows[0].find_all('td')
        if len(dep_cells) >= 4:
            data['departure_time'] = dep_cells[3].text.strip()

        arr_cells = valid_rows[-1].find_all('td')
        if len(arr_cells) >= 4:
            data['arrival_time'] = arr_cells[3].text.strip()
            
    except IndexError:
        log.warning("IndexError while parsing stops table rows.")
    except Exception as e:
        log.error(f"Unexpected error in _parse_stops_table: {e}")


def _parse_details_from_trip_html(trip_html: str) -> Optional[Dict[str, Any]]:
    """
    Helper to parse the detailed HTML from call_load_trip_details.
    Returns a dictionary with extracted data.
    This function no longer returns None if keys are missing.
    """
    if not trip_html:
        return None

    try:
        details_soup = BeautifulSoup(trip_html, 'lxml')
        data: Dict[str, Any] = {}
        
        # 1. Parse Key-Value table (Service Code, Route, Operator, etc.)
        rows = details_soup.find_all('tr')
        details_map = _parse_key_value_table(rows)
        
        data['operator'] = details_map.get("Corporation")
        data['trip_code'] = details_map.get("Service Code")
        data['route_code'] = details_map.get("Route No.")
        data['total_kms'] = details_map.get("Total Kms")
        data['duration'] = details_map.get("Journey Hours")
        
        # 2. Parse Fares (Adult, Child)
        _parse_fares(details_soup, data)
        
        # 3. Parse Stops Table (Departure/Arrival)
        _parse_stops_table(details_soup, data)
        
        # 4. Return whatever was found
        return data
    
    except Exception as e:
        log.error(f"Error parsing trip detail HTML: {e}")
        return None


# Helper to parse the Old HTML (Not the one from load trip details)

def _parse_details_from_bus_div(bus_div: Tag) -> dict:
    """
    Helper with the Old logic to scrape the main list div. - Fallback
    """

    data = {}
    
    # 1. Operator
    operator_element = bus_div.find('span', class_ = 'operator-name')
    data['operator'] = operator_element.text.strip() if operator_element else "N/A"
    
    # 2. Departure Time
    departure_time = "N/A"
    time_info_divs = bus_div.find_all('div', class_='time-info')
    if len(time_info_divs) > 0 and time_info_divs[0]:
        departure_span = time_info_divs[0].find('span')
        if departure_span and departure_span.text is not None:
            departure_time = departure_span.text.strip()
    data['departure_time'] = departure_time
    
    # 3. Arrival Time
    arrival_time = "N/A"
    if len(time_info_divs) > 2 and time_info_divs[2]:
        arrival_span = time_info_divs[2].find('span')
        if arrival_span and arrival_span.text is not None:
            arrival_time = arrival_span.text.strip()
    data['arrival_time'] = arrival_time

    # 4. Duration
    duration = "N/A"
    duration_element = bus_div.find('span', class_='duration')
    if duration_element and duration_element.text is not None:
        duration = duration_element.text.strip().replace('Hrs', '').strip()
    data['duration'] = duration
    
    # 5. Price
    price = 0
    price_div = bus_div.find('div', class_ = 'price')
    if price_div and price_div.contents:
        full_text = " ".join(str(element) for element in price_div.contents)
        tokens = full_text.split()
        try:
            amount = next(t for t in tokens if t.isdigit())
            price = int(amount)
        except (StopIteration, ValueError):
            log.warning("Could not find numeric price in fallback.")
    data['price_in_rs'] = price
    
    # 6. Trip/Route Code
    trip_code, route_code = "N/A", "N/A"
    code_span_parent_candidates = bus_div.find_all('span', class_ = 'text-1 text-muted d-block')
    code_span_parent = next((s for s in code_span_parent_candidates if s.text and '/' in s.text), None)
    if code_span_parent:
        codes_text = code_span_parent.text.strip() if code_span_parent.text is not None else "N/A / N/A"
        parts = codes_text.split('/', 1)
        trip_code = parts[0].strip()
        route_code = parts[1].strip() if len(parts) > 1 else "N/A"
    data['trip_code'] = trip_code
    data['route_code'] = route_code

    return data