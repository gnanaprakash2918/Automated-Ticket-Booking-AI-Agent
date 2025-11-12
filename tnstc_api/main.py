from fastapi import FastAPI, status, Query
from .tnstc_client import get_place_info, parse_bus_results, filter_bus_services
from fastapi.middleware.cors import CORSMiddleware
import logging
import uvicorn
import httpx
from fastapi import FastAPI, HTTPException
from .schemas import SearchRequest, BusSearchResponse, ResponseMetadata
import asyncio
import logging
from utils.logging_setup import setup_logging
from .config import TNSTC_BASE_URL, PARSER_STRATEGY
from typing import Optional
from datetime import datetime

setup_logging()
log = logging.getLogger(__name__)

# Initialize FastAPI App
app = FastAPI(
    title = "TNSTC API Wrapper",
    description = "A FastAPI wrapper for the TNSTC booking website",
    version = "1.0.0",
)

DEVELOPMENT_ORIGINS = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_credentials = True,
    allow_methods = ['GET', 'POST'],
    allow_headers = ['*'],
    allow_origins = DEVELOPMENT_ORIGINS,
)

# Endpoints

@app.get('/', tags = ['Health'])
async def check_health():
    logging.info('Health Check Endpoint was hit.')

    return {
        "status" : "ok",
        "message" : "TNSTC API Wrapper is running."
    }

@app.post("/search_buses", response_model=BusSearchResponse, status_code=status.HTTP_200_OK) 
async def search_buses(
    request: SearchRequest,
    limit: Optional[int] = Query(
        default=None,
        gt=0,
        title="Limit Parsed Results",
        description="Process and return only the first 'n' bus services found."
    )
):
    """
    Performs the full, multi-step bus search against the external TNSTC API, and then filters the results.
    """

    search_time = datetime.now()

    async with httpx.AsyncClient(timeout=30.0) as client:        
        try:
            from_place_task = get_place_info(client, request.from_place_name, is_from_place=True)
            to_place_task = get_place_info(client, request.to_place_name, is_from_place=False)
            
            from_place, to_place = await asyncio.gather(from_place_task, to_place_task)
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                                detail=f"Unexpected error during place identification: {e}")

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
            'txtReturnDate': request.return_date,
            'hiddenOnwardJourneyDate': request.onward_date,
            'hiddenReturnJourneyDate': request.return_date,
            'hiddenAction': 'SearchService', 
            
            # Hardcoded fields
            'languageType': 'E',
            'checkSingleLady': 'N',

            # Include other necessary but empty fields
            'selectOnwardTimeSlab': '', 'hiddenTotalMales': '', 'txtAdultMales': '', 'txtChildMales': '',
            'txtAdultFemales': '', 'txtChildFemales': '', 'hiddenTotalFemales': '', 'selectClass': '',
            'hiddenOnwardTimeSlab': '', 'hiddenClassCategoryLookupID': '', 'chkTatkal': '',
            'hiddenClassName': '', 'matchPStartPlace': '', 'matchPEndPlace': '', 'txtdeptDatePtrip': '',
            'txtUserLoginID': '', 'txtPassword': '', 'txtCaptchaCode': '', 'txtRUserLoginID': '',
            'txtRMobileNo': '', 'txtRUserFullName': '', 'txtRPassword': '',
        }

        try:
            final_url = TNSTC_BASE_URL + "hiddenAction=SearchService"
            response = await client.post(final_url, data=payload)
            response.raise_for_status()

            bus_list = await parse_bus_results(client, response.text, limit)
            
            total_found = len(bus_list)
            
            filtered_bus_list = filter_bus_services(bus_list, request) 
            
            if not filtered_bus_list:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, 
                                    detail="No bus services found matching the specified route, date, and filters.")
                        
            # 1. Create the metadata object
            metadata_obj = ResponseMetadata(
                search_timestamp=search_time,
                parser_strategy=PARSER_STRATEGY,
                total_services_found_before_filtering=total_found,
                limit_applied=limit
            )
            
            # 2. Construct and return the final response
            return BusSearchResponse(
                metadata=metadata_obj,
                from_place=from_place,
                to_place=to_place,
                services=filtered_bus_list
            )

        except httpx.HTTPStatusError as e:
             error_detail = f"External search API returned status {e.response.status_code}. The search may be temporarily unavailable."
             raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=error_detail)
        except httpx.RequestError as e:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"External API network request failed: {e}")

if __name__ == "__main__":
    uvicorn.run("tnstc_api.main:app", host="localhost", port=9000, reload=True)