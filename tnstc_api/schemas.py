from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Type, Dict, Any, Tuple, cast
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
    via_route: Optional[List[str]] = Field(default=None, description="List of key intermediate stops on the route.")

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
                    "via_route": ["TIRUPATHUR", "VELLORE"],
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

    @field_validator('child_fare')
    @classmethod
    def set_child_fare_na_if_none(cls, v: Optional[str]) -> str:
        """Converts a null/None child_fare to 'NA'."""
        if v is None:
            return "NA"
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
                            "via_route": ["TIRUPATHUR", "VELLORE"],
                            "total_kms": "308.00",
                            "child_fare": "NA"
                        }
                    ]
                }
            ]
        }
    }

# Structured LLM Output

class BusServiceList(BaseModel):
    """A container model to ask the LLM for a list of bus services."""
    services: List[BusService] = Field(default_factory=list, description="A list of all bus services found on the page.")

# Gemini Schema Conversion Utility

TYPE_MAP = {
    "string": "STRING",
    "integer": "INTEGER",
    "number": "NUMBER",
    "boolean": "BOOLEAN",
    "object": "OBJECT",
    "array": "ARRAY",
}


def _resolve_schema_refs(schema: Dict[str, Any], definitions: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively resolve $ref pointers in a JSON schema.

    - If a schema contains a $ref, replace it with the resolved definition.
    - Recurse into nested "properties", "items", and "anyOf" sections.
    """

    if "$ref" in schema:
        ref_key = schema["$ref"].split("/")[-1]
        ref_schema = cast(Dict[str, Any], definitions.get(ref_key, {}))
        return _resolve_schema_refs(ref_schema, definitions)

    if "properties" in schema:
        schema["properties"] = {
            k: _resolve_schema_refs(v, definitions)
            for k, v in schema["properties"].items()
        }

    if "items" in schema:
        schema["items"] = _resolve_schema_refs(schema["items"], definitions)

    if "anyOf" in schema:
        schema["anyOf"] = [_resolve_schema_refs(s, definitions) for s in schema["anyOf"]]

    return schema


def _collapse_optional_schema(schema: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    """
    Collapse Optional[T] represented as {"anyOf": [T, {"type": "null"}]} into (T, nullable=True).
    If not optional, return (schema, False).
    """

    if "anyOf" in schema:
        non_null = next((s for s in schema["anyOf"] if s.get("type") != "null"), None)
        if non_null:
            return _collapse_optional_schema(non_null) + (True,)  # type: ignore[return-value]
        return {**schema, "nullable": True}, True  # type: ignore[return-value]
    return schema, False


def _convert_pydantic_to_gemini(p_schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively converts a Pydantic JSON schema to the Gemini-compatible format.

    - Uppercases primitive types.
    - Converts nested objects/arrays.
    - Narrows down Optional types via _collapse_optional_schema.
    - Preserves descriptions when present.
    """

    g_schema: Dict[str, Any] = {}

    if "description" in p_schema:
        g_schema["description"] = p_schema["description"]

    base_schema, is_nullable = _collapse_optional_schema(p_schema)

    if isinstance(base_schema, dict):
        p_type = base_schema.get("type")
    else:
        p_type = None

    if "type" in base_schema:
        p_type = base_schema.get("type")

    if p_type == "object":
        g_schema["type"] = "OBJECT"
        g_schema["properties"] = {
            k: _convert_pydantic_to_gemini(v)
            for k, v in cast(Dict[str, Any], base_schema.get("properties", {})).items()
        }
        if "required" in base_schema:
            g_schema["required"] = base_schema["required"]
    elif p_type == "array":
        g_schema["type"] = "ARRAY"
        g_schema["items"] = _convert_pydantic_to_gemini(base_schema.get("items", {}))
    elif p_type == "string":
        g_schema["type"] = "STRING"
        if "enum" in base_schema:
            g_schema["enum"] = base_schema["enum"]
    elif p_type == "integer":
        g_schema["type"] = "INTEGER"
    elif p_type == "number":
        g_schema["type"] = "NUMBER"
    elif p_type == "boolean":
        g_schema["type"] = "BOOLEAN"
    else:
        g_schema["type"] = "STRING"

    if is_nullable:
        g_schema["nullable"] = True

    return g_schema


def get_gemini_schema_for(model: Type[BaseModel]) -> Dict[str, Any]:
    """
    Generates a Gemini-compatible, $ref-resolved JSON schema for a Pydantic model.

    Process:
    1) Generate Pydantic JSON schema
    2) Resolve all $ref pointers
    3) Convert to Gemini format
    """

    pydantic_schema = model.model_json_schema()
    definitions = cast(Dict[str, Any], pydantic_schema.get("$defs", {}))
    resolved = _resolve_schema_refs(pydantic_schema, definitions)

    root_properties = cast(Dict[str, Any], resolved.get("properties", {}))
    gemini_schema = _convert_pydantic_to_gemini({"type": "object", "properties": root_properties})
    return gemini_schema