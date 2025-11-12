import os
from dotenv import load_dotenv
from typing import Literal, Optional

load_dotenv()

# "production" in your deployment environment
APP_ENV: str = os.getenv("APP_ENV", "development")

LOG_DIR: str = "logs"

TNSTC_BASE_URL: str = os.getenv('TNSTC_BASE_URL', 'https://www.tnstc.in/OTRSOnline/jqreq.do?')
TNSTC_DETAILS_URL: str = "https://www.tnstc.in/OTRSOnline/advanceNewBooking.do"

ParserStrategy = Literal["beautifulsoup", "gemini", "ollama"]
PARSER_STRATEGY: ParserStrategy = os.getenv("PARSER_STRATEGY", "beautifulsoup") # type: ignore

GEMINI_API_KEY: Optional[str] = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-preview-09-2025")
GEMINI_API_URL: str = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

OLLAMA_BASE_URL: str = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL: str = os.getenv("OLLAMA_MODEL", "llama3:8b")
OLLAMA_API_URL: str = f"{OLLAMA_BASE_URL}/api/generate"

OLLAMA_CONCURRENCY_LIMIT: int = int(os.getenv("OLLAMA_CONCURRENCY_LIMIT", "5"))