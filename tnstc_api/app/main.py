from fastapi import FastAPI, status
from tnstc_client import get_place_info, parse_bus_results, filter_bus_services
from fastapi.middleware.cors import CORSMiddleware
import logging
import uvicorn
import httpx
from fastapi import FastAPI, HTTPException
from schemas import SearchRequest, BusSearchResponse

# Setup Logging Config
logging.basicConfig(level = logging.INFO)

# Initialize FastAPI App
app = FastAPI(
    title = "TNSTC API Wrapper",
    description = "A FastAPI wrapper for the TNSTC booking website",
    version = "1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_credentials = True,
    allow_methods = ['GET', 'POST'],
    allow_headers = ['*'],

    # Change to frontend after completion
    allow_origins = ['*'],
)

@app.get('/', tags = ['Health'])
async def check_health():
    logging.info('Health Check Endpoint was hit.')

    return {
        "status" : "ok",
        "message" : "TNSTC API Wrapper is running."
    }

BASE_URL = "https://www.tnstc.in/OTRSOnline/jqreq.do?"

@app.post("/search_buses", response_model=BusSearchResponse, status_code=status.HTTP_200_OK) 
async def search_buses(request: SearchRequest):
    """
    Performs the full, multi-step bus search against the external TNSTC API, and then filters the results.
    """

    async with httpx.AsyncClient(timeout=30.0) as client:        
        try:
            from_place = await get_place_info(client, request.from_place_name, is_from_place=True)
            to_place = await get_place_info(client, request.to_place_name, is_from_place=False)
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

            # 'hiddenCurrentDate': '09/11/2025', 
            # 'hiddenMaxNoOfPassengers': '16',
            # 'hiddenMaxValidReservDate': '9/12/2025',
            # 'hiddenUserType': 'G',

            # Include other necessary but empty fields
            'selectOnwardTimeSlab': '', 'hiddenTotalMales': '', 'txtAdultMales': '', 'txtChildMales': '',
            'txtAdultFemales': '', 'txtChildFemales': '', 'hiddenTotalFemales': '', 'selectClass': '',
            'hiddenOnwardTimeSlab': '', 'hiddenClassCategoryLookupID': '', 'chkTatkal': '',
            'hiddenClassName': '', 'matchPStartPlace': '', 'matchPEndPlace': '', 'txtdeptDatePtrip': '',
            'txtUserLoginID': '', 'txtPassword': '', 'txtCaptchaCode': '', 'txtRUserLoginID': '',
            'txtRMobileNo': '', 'txtRUserFullName': '', 'txtRPassword': '',
        }

        try:
            final_url = BASE_URL + "hiddenAction=SearchService"
            response = await client.post(final_url, data=payload)
            response.raise_for_status()

            bus_list = parse_bus_results(response.text)
            
            filtered_bus_list = filter_bus_services(bus_list, request) 
            
            if not filtered_bus_list:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, 
                                    detail="No bus services found matching the specified route, date, and filters.")
            
            # Construct and return the new response object with IDs and Codes
            return BusSearchResponse(
                from_place_name=from_place.name,
                from_place_id=from_place.id,
                from_place_code=from_place.code,
                to_place_name=to_place.name,
                to_place_id=to_place.id,
                to_place_code=to_place.code,
                services=filtered_bus_list
            )

        except httpx.HTTPStatusError as e:
             error_detail = f"External search API returned status {e.response.status_code}. The search may be temporarily unavailable."
             raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=error_detail)
        except httpx.RequestError as e:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"External API network request failed: {e}")

if __name__ == "__main__":
    uvicorn.run("main:app", host = "localhost", port = 9000, reload = True)