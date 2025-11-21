import asyncio
from datetime import datetime
from pathlib import Path
import sys
from typing import Any, Dict, List, Type
import httpx
from loguru import logger
from rich.console import Console
from rich.panel import Panel
from rich.table import Table


# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from test_config import (  # noqa: E402
    CRITICAL_FIELDS,
    LIMIT_BUSES,
    SERVICE_TEST_DATA,
    TEST_DATE,
    TEST_SERVICE,
)
from services.tnstc.parsers.base import AbstractBusParser  # noqa: E402
from services.tnstc.parsers.bs_parser import BeautifulSoupParser  # noqa: E402
from services.tnstc.parsers.gemini_parser import GeminiParser  # noqa: E402
from services.tnstc.parsers.ollama_parser import OllamaParser  # noqa: E402
from services.tnstc.schemas import TNSTCBusService, TNSTCSearchRequest  # noqa: E402
from services.tnstc.service import TNSTCService  # noqa: E402
from utils.logger import setup_logging  # noqa: E402


# Configuration
PARSERS_MAP: Dict[str, Type[AbstractBusParser]] = {
    "beautifulsoup": BeautifulSoupParser,
    "gemini": GeminiParser,
    "ollama": OllamaParser,
}

console = Console()
service_instance = TNSTCService()

# Directory for saving debug HTML
DEBUG_DIR = Path(__file__).parent / "retrieved_htmls"
DEBUG_DIR.mkdir(exist_ok=True, parents=True)


async def fetch_live_html(client: httpx.AsyncClient) -> str:
    """Orchestrates the service to fetch real HTML from TNSTC."""
    console.print(f"[bold blue]Fetching live data for {TEST_SERVICE}...[/bold blue]")

    test_data = SERVICE_TEST_DATA[TEST_SERVICE]
    request = TNSTCSearchRequest(
        from_place_name=test_data["from_place"],
        to_place_name=test_data["to_place"],
        onward_date=TEST_DATE,
    )

    await service_instance.initialize_db()

    # 1. Resolve Places
    from_place = await service_instance._fetch_place_info(
        request.from_place_name, is_from_place=True
    )
    to_place = await service_instance._fetch_place_info(
        request.to_place_name, is_from_place=False
    )

    # 2. Construct Payload
    payload = service_instance._construct_search_payload(from_place, to_place, request)

    # 3. Request
    url = f"{service_instance.base_url}?hiddenAction=SearchService"
    response = await client.post(url, data=payload)
    response.raise_for_status()

    # Save HTML for debugging
    timestamp = datetime.now().strftime("%H%M%S")
    with open(DEBUG_DIR / f"live_search_{timestamp}.html", "w", encoding="utf-8") as f:
        f.write(response.text)

    return response.text


async def run_parser_safe(
    name: str,
    parser_cls: Type[AbstractBusParser],
    client: httpx.AsyncClient,
    html: str,
    limit: int,
) -> List[TNSTCBusService]:
    """Runs a parser safely and handles exceptions."""
    console.print(
        f"   -> Running [bold magenta]{name}[/bold magenta] parser (Limit: {limit})..."
    )
    try:
        parser = parser_cls()
        results = await parser.parse(client, html, limit=limit)
        return results
    except Exception:
        logger.exception(f"Parser {name} failed")
        return []


def normalize_value(val: Any) -> str:
    """Normalizes values for comparison (strips whitespace, handles None)."""
    if val is None:
        return "N/A"
    return str(val).strip()


async def main_test_runner():
    setup_logging()

    async with httpx.AsyncClient(timeout=45.0) as client:
        # 1. Get Data
        try:
            html_content = await fetch_live_html(client)
        except Exception as e:
            console.print(f"[bold red]Failed to fetch HTML:[/bold red] {e}")
            return

        # 2. Run All Parsers
        results: Dict[str, List[TNSTCBusService]] = {}

        for name, cls in PARSERS_MAP.items():
            results[name] = await run_parser_safe(
                name, cls, client, html_content, LIMIT_BUSES
            )

    # 3. Validate Limits and counts
    console.rule("[bold yellow]Limit & Count Validation[/bold yellow]")

    bs_results = results.get("beautifulsoup", [])
    if not bs_results:
        console.print(
            "[bold red]CRITICAL: BeautifulSoup returned 0 results. Aborting comparison.[/bold red]"
        )
        return

    for name, items in results.items():
        count = len(items)
        style = "green" if count <= LIMIT_BUSES and count > 0 else "red"
        console.print(
            f"Parser [bold]{name}[/bold]: Found [{style}]{count}[/{style}] buses (Limit was {LIMIT_BUSES})"
        )

    # 4. Detailed Comparison
    console.rule("[bold yellow]Consistency Analysis[/bold yellow]")

    # Track overall stats
    stats = {
        name: {"match": 0, "mismatch": 0, "missing": 0}
        for name in results
        if name != "beautifulsoup"
    }

    for idx, ref_service in enumerate(bs_results):
        trip_code = ref_service.trip_code

        # Create a row for this bus
        bus_panel_title = f"Bus #{ref_service.bus_number} | Code: {trip_code} | {ref_service.operator}"

        table = Table(box=None, padding=(0, 1), show_header=True)
        table.add_column("Field", style="dim")
        table.add_column("BS (Ref)", style="bold white")

        other_parsers = [p for p in results.keys() if p != "beautifulsoup"]
        for p in other_parsers:
            table.add_column(p.title())

        # Find matching service in other parsers
        matches = {}
        has_diff = False

        for p_name in other_parsers:
            # Try finding by trip_code first
            p_items = results.get(p_name, [])
            match = next((x for x in p_items if x.trip_code == trip_code), None)

            # Fallback: index matching
            if (
                not match
                and idx < len(p_items)
                and (trip_code == "N/A" or p_items[idx].trip_code == "N/A")
            ):
                match = p_items[idx]

            matches[p_name] = match

        # Compare Fields
        # FIX: Use class access for model_fields to avoid Pydantic warning
        all_fields = [
            f
            for f in TNSTCBusService.model_fields.keys()
            if f not in ["llm_reasoning", "metadata"]
        ]

        for field in all_fields:
            ref_val = normalize_value(getattr(ref_service, field))
            row_cells = [field, ref_val]
            field_has_issue = False

            for p_name in other_parsers:
                target = matches[p_name]
                if not target:
                    row_cells.append("[red]MISSING[/red]")
                    field_has_issue = True
                    continue

                target_val = normalize_value(getattr(target, field))

                if field == "price_in_rs":
                    try:
                        if abs(float(ref_val) - float(target_val)) < 1.0:
                            row_cells.append(f"[green]{target_val}[/green]")
                            continue
                    except ValueError:
                        pass

                if target_val == ref_val:
                    row_cells.append(f"[green]{target_val}[/green]")
                else:
                    style = "bold red" if field in CRITICAL_FIELDS else "yellow"
                    row_cells.append(f"[{style}]{target_val}[/{style}]")
                    field_has_issue = True

            if field_has_issue:
                has_diff = True
                table.add_row(*row_cells)

        # Update stats
        for p_name in other_parsers:
            if not matches[p_name]:
                stats[p_name]["missing"] += 1

        if has_diff:
            console.print(
                Panel(
                    table,
                    title=f"[yellow]{bus_panel_title}[/yellow]",
                    border_style="yellow",
                )
            )
        else:
            console.print(f"[green]✔ {bus_panel_title} - Fully Consistent[/green]")

    console.rule("[bold blue]Summary[/bold blue]")
    for p_name in other_parsers:
        missing = stats[p_name]["missing"]
        total = len(bs_results)
        found = total - missing
        console.print(f"{p_name.title()}: Found {found}/{total} matching buses.")


if __name__ == "__main__":
    try:
        asyncio.run(main_test_runner())
    except KeyboardInterrupt:
        console.print("[red]Test interrupted by user.[/red]")
