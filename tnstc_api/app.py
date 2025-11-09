from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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