from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import datetime
import re

class PlaceInfo(BaseModel):
    """Internal model used to store the parsed ID, Code, and Name for a location."""

    id: str = Field(default=..., description="The internal TNSTC ID for the place (digits only).")
    code: str = Field(default=..., description="Three-letter uppercase TNSTC code.")
    name: str = Field(default=..., description="Full normalized name of the place.")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"id": "488", "code": "DHA", "name": "DHARMAPURI"}
            ]
        }
    }

    @field_validator('id')
    @classmethod
    def id_must_be_numeric(cls, v: str) -> str:
        if not v.isdigit():
            raise ValueError('id must only contain digits')
        return v

    @field_validator('code')
    @classmethod
    def code_must_be_three_uppercase_letters(cls, v: str) -> str:
        if not re.fullmatch(r'[A-Z]{3}', v):
            raise ValueError('code must be exactly three uppercase letters')
        return v


class BusService(BaseModel):
    """Output model representing a single available bus service and its details."""

    operator: str = Field(default=..., description="Name of the operating corporation.")
    bus_type: str = Field(default=..., description="Type or class of the bus.")
    trip_code: str = Field(default=..., description="Unique service code.")
    route_code: str = Field(default=..., description="TNSTC internal route identifier.")
    departure_time: str = Field(default=..., description="Scheduled departure time in 24-hour format.")
    arrival_time: str = Field(default=..., description="Scheduled arrival time in 24-hour format.")
    duration: str = Field(default=..., description="Total journey duration in hours, decimal allowed.")
    price_in_rs: int = Field(default=..., description="Base ticket price in Rupees.")
    seats_available: int = Field(default=..., description="Number of available seats.")
    via_route: Optional[str] = Field(default=None, description="Key intermediate stops on the route.")

    total_kms: Optional[str] = Field(default=None, description="Approximate total distance in kilometers.")
    child_fare: Optional[str] = Field(default=None, description="Child fare, if available (can be 'NA').")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "operator": "SALEM",
                    "bus_type": "AC 3X2",
                    "trip_code": "2215DHACHEDD02A",
                    "route_code": "275H",
                    "departure_time": "22:15",
                    "arrival_time": "04:50",
                    "duration": "7.45",
                    "price_in_rs": 350,
                    "seats_available": 20,
                    "via_route": "TIRUPATHUR,VELLORE",
                    "total_kms": "308.00",
                    "child_fare": "NA"
                }
            ]
        }
    }

    @field_validator('departure_time', 'arrival_time')
    @classmethod
    def validate_time_format(cls, v: str) -> str:
        if not re.fullmatch(r'([01]\d|2[0-3]):[0-5]\d', v):
            raise ValueError('time must be in HH:MM 24-hour format')
        return v

    @field_validator('duration')
    @classmethod
    def validate_and_normalize_duration(cls, v: str) -> str:
        """
        Validates duration is positive and normalizes it to a float string.
        Handles both "HH:MM" (e.g., "7:30") and float-string (e.g., "7.45").
        """
        if ':' in v:
            # Handle "HH:MM" format from the new parser
            try:
                hours, minutes = v.split(':')
                total_hours = int(hours) + (int(minutes) / 60)
                if total_hours <= 0:
                     raise ValueError('duration must be positive')
                
                # Return as standardized float string, e.g., "7.50"
                return f"{total_hours:.2f}" 
            except Exception:
                raise ValueError('invalid HH:MM duration format')
        else:
            # Handle float string format (e.g., "7.45") from the old parser
            try:
                if float(v) <= 0:
                    raise ValueError('duration must be positive')
                
                # Already in the correct format
                return v
            except ValueError:
                raise ValueError('duration must be a valid float string or HH:MM')

    @field_validator('price_in_rs', 'seats_available')
    @classmethod
    def non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError('must be non-negative')
        return v


class SearchRequest(BaseModel):
    """Input model defining the required parameters for a bus search, now including optional filters."""

    from_place_name: str = Field(default=..., description="Starting city name.")
    to_place_name: str = Field(default=..., description="Destination name.")
    onward_date: str = Field(default=..., description="Travel date in DD/MM/YYYY.")
    return_date: Optional[str] = Field(default="DD/MM/YYYY", description="Optional return date or one-way.")

    # Filter Fields
    min_price_in_rs: Optional[int] = Field(default=100, description="Minimum allowed ticket price in Rupees (default: 100).")
    max_price_in_rs: Optional[int] = Field(default=1000, description="Maximum allowed ticket price in Rupees (default: 1000).")
    
    min_departure_time: Optional[str] = Field(default="00:00", description="Earliest desired departure time (HH:MM, default: 00:00).") 
    max_departure_time: Optional[str] = Field(default="23:59", description="Latest desired departure time (HH:MM, default: 23:59).") 
    
    allowed_bus_types: Optional[List[str]] = Field(
        default=None, 
        description="List of preferred bus type strings. If None, all types are allowed."
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "from_place_name": "Dharmapuri",
                    "to_place_name": "CHENNAI-PT DR. M.G.R. BS",
                    "onward_date": "09/11/2025",
                    "return_date": "15/11/2025",
                    "min_price_in_rs": 200,
                    "max_price_in_rs": 800,
                    "min_departure_time": "18:00",
                    "max_departure_time": "23:59",
                    "allowed_bus_types": ["AC SLEEPER", "ULTRA DELUXE"]
                }
            ]
        }
    }

    @field_validator('from_place_name', 'to_place_name')
    @classmethod
    def names_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError('place name must not be empty')
        return v

    @field_validator('onward_date', 'return_date', mode='before')
    @classmethod
    def validate_date_format(cls, v: Optional[str]) -> Optional[str]:
        if v and v != "DD/MM/YYYY":
            datetime.strptime(v, '%d/%m/%Y')
        return v

    @field_validator('min_departure_time', 'max_departure_time', mode='before')
    @classmethod
    def validate_time_format(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not re.fullmatch(r'([01]\d|2[0-3]):[0-5]\d', v):
            raise ValueError('time must be in HH:MM 24-hour format')
        return v
    
    @field_validator('min_price_in_rs', 'max_price_in_rs', mode='before')
    @classmethod
    def non_negative_price(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 0:
            raise ValueError('price must be non-negative')
        return v


class BusSearchResponse(BaseModel):
    """
    Final output model for the bus search, including metadata like place names and internal codes.
    """
    from_place_name: str = Field(..., description="The confirmed starting city name.")
    from_place_id: str = Field(..., description="Internal ID for the starting place.")
    from_place_code: str = Field(..., description="Three-letter code for the starting place.")
    to_place_name: str = Field(..., description="The confirmed destination city name.")
    to_place_id: str = Field(..., description="Internal ID for the destination place.")
    to_place_code: str = Field(..., description="Three-letter code for the destination place.")
    services: List[BusService] = Field(..., description="List of available bus services.")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "from_place_name": "DHARMAPURI",
                    "from_place_id": "488",
                    "from_place_code": "DHA",
                    "to_place_name": "CHENNAI-PT DR. M.G.R. BS",
                    "to_place_id": "275",
                    "to_place_code": "CHEDD",
                    "services": [
                        {
                            "operator": "SALEM",
                            "bus_type": "AC 3X2",
                            "trip_code": "2215DHACHEDD02A",
                            "route_code": "275H",
                            "departure_time": "22:15",
                            "arrival_time": "04:50",
                            "duration": "7.45",
                            "price_in_rs": 350,
                            "seats_available": 20,
                            "via_route": "TIRUPATHUR,VELLORE",
                            "total_kms": "308.00",
                            "child_fare": "NA"
                        }
                    ]
                }
            ]
        }
    }