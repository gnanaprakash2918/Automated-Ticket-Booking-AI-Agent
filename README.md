# TNSTC API Wrapper

A robust, async Python wrapper for the TNSTC (Tamil Nadu State Transport Corporation) bus booking service. This project uses a hybrid parsing strategy, combining BeautifulSoup for speed and LLMs (Gemini/Ollama) for resilience against DOM changes.

**Status:** Phase 1 Complete

## Key Features

-   **Hybrid Parsing Engine:** Automatically switches between selector-based parsing (BeautifulSoup) and AI-based parsing (Gemini/Ollama) based on configuration.
-   **Smart Pre-filtering:** Filters bus results by price, time, and bus type *before* sending data to the LLM, significantly reducing token usage and latency.
-   **Concurrency Control:** Implements Semaphores to strictly limit concurrent LLM requests, preventing rate-limit errors.
-   **Persistent Caching:** SQLite-backed caching for place lookups (`tnstc_places`) to minimize external API calls.
-   **Ambiguity Resolution:** Handles ambiguous place names (e.g., "Chen" -> "CHENNAI", "CHENGALPATTU") via a dedicated search endpoint.
-   **Dynamic Architecture:** Modular design allowing easy addition of new transport providers or LLM backends.

## Setup

1.  **Environment:**
    ```bash
    python -m venv .venv
    # Windows:
    .\.venv\Scripts\activate
    # Linux/Mac:
    source .venv/bin/activate
    
    pip install -r requirements.txt
    ```

2.  **Configuration:**
    Create a `.env` file:
    ```env
    APP_ENV="development"
    PARSER_STRATEGY="gemini" # Options: beautifulsoup, gemini, ollama
    LLM_PROVIDER="gemini"
    
    # Gemini Config
    GEMINI_API_KEY="YOUR_KEY_HERE"
    GEMINI_MODEL="gemini-1.5-flash"
    GEMINI_CONCURRENCY_LIMIT=5
    
    # Ollama Config
    OLLAMA_BASE_URL="http://localhost:11434"
    OLLAMA_CONCURRENCY_LIMIT=2
    ```

3.  **Run:**
    ```bash
    python main.py
    ```

## API Usage & Testing

Import these cURLs into Postman or run them directly in your terminal.

### 1. System Health
Check API status and current configuration.

```bash
curl --location 'http://localhost:9000/health'
````

```bash
curl --location 'http://localhost:9000/config_info'
```

### 2\. Place Search

Find available cities (caches results automatically).

```bash
curl --location 'http://localhost:9000/places/search?query=Che'
```

### 3\. Bus Search

**Basic Search:**

```bash
curl --location 'http://localhost:9000/search' \
--header 'Content-Type: application/json' \
--data '{
    "from_place_name": "DHARMAPURI",
    "to_place_name": "CHENNAI-PT DR. M.G.R. BS",
    "onward_date": "25/12/2025"
}'
```

**Filtered Search (Price, Time, Type):**

```bash
curl --location 'http://localhost:9000/search' \
--header 'Content-Type: application/json' \
--data '{
    "from_place_name": "SALEM",
    "to_place_name": "MADURAI",
    "onward_date": "20/12/2025",
    "min_price_in_rs": 150,
    "max_price_in_rs": 600,
    "min_departure_time": "18:00",
    "max_departure_time": "23:59",
    "allowed_bus_types": ["ULTRA DELUXE", "AC SLEEPER"]
}'
```

**Search with Limit (Optimization):**
Returns only the first N results to save processing time.

```bash
curl --location 'http://localhost:9000/search?limit=2' \
--header 'Content-Type: application/json' \
--data '{
    "from_place_name": "COIMBATORE",
    "to_place_name": "SALEM",
    "onward_date": "25/12/2025"
}'
```

**Direct Search (Bypass Lookup):**
Fastest method if you already know the TNSTC Place IDs/Codes.

```bash
curl --location 'http://localhost:9000/search' \
--header 'Content-Type: application/json' \
--data '{
    "from_place_name": "DHARMAPURI",
    "from_place_id": "488",
    "from_place_code": "DHA",
    "to_place_name": "CHENNAI",
    "to_place_id": "275",
    "to_place_code": "CHEDD",
    "onward_date": "25/12/2025"
}'
```

## Development Tools

  - **Format:** `ruff format .`
  - **Lint:** `ruff check . --fix`
  - **Run Parser Tests:** `python tests/parsers_test.py`
  - **Create Migration:** `python utils/create_migration.py "description_here"`
