"""
Configuration for parser tests.
Allows easy switching between different transport services.
"""

from datetime import date


# Service to test (currently: "tnstc")
# Future options: "redbus", "irctc", etc.
TEST_SERVICE = "tnstc"

# Test parameters
LIMIT_BUSES = 5
TEST_DATE = date(2025, 12, 20).strftime("%d/%m/%Y")

# Service-specific test data
SERVICE_TEST_DATA = {
    "tnstc": {
        "from_place": "DHARMAPURI",
        "to_place": "CHENNAI-PT DR. M.G.R. BS",
        "min_price_in_rs": 200,
        "max_price_in_rs": 400,
        "min_departure_time": "18:00",
        "max_departure_time": "23:59",
    }
    # Future services can be added here:
    # "redbus": {
    #     "from_place": "Bangalore",
    #     "to_place": "Chennai",
    # }
}

# Field configurations for comparison
CRITICAL_FIELDS = [
    "trip_code",
    "route_code",
    "bus_type",
    "departure_time",
    "arrival_time",
    "duration",
]

NON_CRITICAL_FIELDS = [
    "operator",
    "price_in_rs",
    "seats_available",
    "via_route",
    "total_kms",
    "child_fare",
]
