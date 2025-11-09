from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

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