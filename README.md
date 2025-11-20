# TNSTC API Wrapper (Phase 1)

A robust, async Python wrapper for the TNSTC (Tamil Nadu State Transport Corporation) bus booking service, powered by LLMs for intelligent parsing.

## Features (Phase 1)

-   **Hybrid Parsing Strategy:** Combines high-speed `BeautifulSoup` parsing with intelligent `LLM` (Gemini/Ollama) parsing for complex details.
-   **Persistent Caching:** SQLite-backed caching for place lookups (`tnstc_places`), reducing API calls and improving speed.
-   **Smart Pre-filtering:** Filters buses by price, time, and type *before* expensive LLM processing.
-   **LLM Abstraction:** Easily switch between Gemini and Ollama providers using a Factory pattern.
-   **Robust Logging:** Detailed, colorful logs using `loguru`.
-   **Ambiguity Resolution:** `search_places` API to handle ambiguous place names (e.g., "Chen" -> "CHENNAI", "CHENGALPATTU").

## Setup

1.  **Environment:**
    ```bash
    python -m venv .venv
    .\.venv\Scripts\activate
    pip install -r requirements.txt
    ```

2.  **Configuration:**
    Create a `.env` file with:
    ```env
    GEMINI_API_KEY=your_key_here
    GEMINI_MODEL=gemini-1.5-flash
    PARSER_STRATEGY=gemini  # or 'ollama', 'beautifulsoup'
    ```

3.  **Run:**
    ```bash
    python main.py
    ```

## Architecture Highlights

-   **Service Layer:** `TNSTCService` orchestrates the search flow.
-   **Parsers:**
    -   `BeautifulSoupParser`: Fast, selector-based parsing.
    -   `GeminiParser` / `OllamaParser`: LLM-based parsing for unstructured data.
-   **Database:** `aiosqlite` for async SQLite interactions.
-   **LLM Interface:** `LLMFactory` creates instances based on config, using external prompt templates.

## Commands

-   **Format:** `ruff format .`
-   **Lint:** `ruff check . --fix`
-   **Clean Cache:** `Get-ChildItem -Path . -Recurse -Include '__pycache__', '*.pyc' | Remove-Item -Recurse -Force`
