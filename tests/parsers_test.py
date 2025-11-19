import asyncio
from datetime import date
import logging
from pathlib import Path
from typing import Any, Dict, List
import httpx
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from tnstc_api.parsers.bs_parser import BeautifulSoupParser
from tnstc_api.parsers.gemini_parser import GeminiParser
from tnstc_api.parsers.ollama_parser import OllamaParser
from tnstc_api.schemas import BusService, SearchRequest
from tnstc_api.tnstc_client import get_place_info
from utils.logging_setup import setup_logging


OUT_HTML_LOG = Path(__file__).with_name("retrieved_htmls.txt")

TEST_DATE = date(2025, 12, 20).strftime("%d/%m/%Y")
TEST_REQUEST = SearchRequest(
    from_place_name="DHARMAPURI",
    to_place_name="CHENNAI-PT DR. M.G.R. BS",
    onward_date=TEST_DATE,
)

LIMIT_BUSES = 5

log = logging.getLogger("ConsistencyTestRunner")
console = Console()
PARSERS_MAP = {
    "beautifulsoup": BeautifulSoupParser,
    "gemini": GeminiParser,
    "ollama": OllamaParser,
}

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


async def append_html_to_log(
    lock: asyncio.Lock, name: str, html: str, out_path: Path = OUT_HTML_LOG
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    async with lock:
        with open(out_path, "a", encoding="utf-8") as fh:
            fh.write(f"{name}\n")
            fh.write(html)
            fh.write("\n\n---HTML_BLOCK_END---\n\n")


def compare_service_fields(
    service_a: BusService, service_b: BusService, parser_a: str, parser_b: str
) -> Dict[str, Any]:
    diffs = {}

    dict_a = service_a.model_dump(exclude_none=True, mode="json")
    dict_b = service_b.model_dump(exclude_none=True, mode="json")

    all_keys = set(dict_a.keys()) | set(dict_b.keys())

    for key in all_keys:
        val_a = dict_a.get(key)
        val_b = dict_b.get(key)

        if val_a == val_b:
            continue

        if key == "price_in_rs" and val_a is not None and val_b is not None:
            if abs(int(val_a) - int(val_b)) <= 1:
                continue

        if isinstance(val_a, list) and isinstance(val_b, list):
            if set(val_a) == set(val_b):
                continue

        is_critical = key in CRITICAL_FIELDS
        diffs[key] = {parser_a: val_a, parser_b: val_b, "critical": is_critical}
    return diffs


def get_comparison_summary(
    all_results: Dict[str, List[BusService]],
) -> List[Dict[str, Any]]:
    bs_results = all_results.get("beautifulsoup", [])
    summary = []

    max_buses = len(bs_results)

    for i in range(max_buses):
        bus_summary = {
            "index": i,
            "reference_trip_code": bs_results[i].trip_code
            if i < len(bs_results)
            else "N/A",
            "bs_service": bs_results[i] if i < len(bs_results) else None,
            "comparisons": {},
        }

        for other_parser_name, other_results in all_results.items():
            if other_parser_name == "beautifulsoup":
                continue

            if i < len(other_results):
                other_service = other_results[i]
                diffs = compare_service_fields(
                    bs_results[i], other_service, "beautifulsoup", other_parser_name
                )
                bus_summary["comparisons"][other_parser_name] = {
                    "diffs": diffs,
                    "service": other_service,
                    "consistent": not bool(diffs),
                }
            else:
                bus_summary["comparisons"][other_parser_name] = {
                    "diffs": {
                        "count": f"Missing (BS={max_buses}, {other_parser_name}={len(other_results)})"
                    },
                    "service": None,
                    "consistent": False,
                }

        summary.append(bus_summary)

    return summary


async def run_parser(
    parser_name: str,
    client: httpx.AsyncClient,
    html_content: str,
    limit: int,
    write_lock: asyncio.Lock,
) -> List[BusService]:
    try:
        await append_html_to_log(write_lock, f"{parser_name}.html", html_content)

        ParserClass = PARSERS_MAP[parser_name]
        parser = ParserClass()
        return await parser.parse(client, html_content, limit)
    except Exception as e:
        log.error(f"FATAL ERROR in {parser_name} parser execution: {e}", exc_info=True)
        return []


async def main_test_runner():
    setup_logging()
    write_lock = asyncio.Lock()

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            from_place, to_place = await asyncio.gather(
                get_place_info(
                    client, TEST_REQUEST.from_place_name, is_from_place=True
                ),
                get_place_info(client, TEST_REQUEST.to_place_name, is_from_place=False),
            )

            payload = {
                "hiddenStartPlaceID": from_place.id,
                "hiddenEndPlaceID": to_place.id,
                "txtStartPlaceCode": from_place.code,
                "txtEndPlace": to_place.code
                if hasattr(to_place, "code")
                else to_place.code,
                "hiddenStartPlaceName": from_place.name,
                "hiddenEndPlaceName": to_place.name,
                "matchStartPlace": from_place.name,
                "matchEndPlace": to_place.name,
                "selectStartPlace": from_place.code,
                "selectEndPlace": to_place.code,
                "txtJourneyDate": TEST_REQUEST.onward_date,
                "hiddenOnwardJourneyDate": TEST_REQUEST.onward_date,
                "hiddenAction": "SearchService",
                "languageType": "E",
                "checkSingleLady": "N",
            }

            initial_search_url = (
                "https://www.tnstc.in/OTRSOnline/jqreq.do?hiddenAction=SearchService"
            )
            response = await client.post(initial_search_url, data=payload)
            response.raise_for_status()
            initial_html = response.text

            await append_html_to_log(write_lock, "initial_search.html", initial_html)

            log.info(
                f"Successfully fetched initial search HTML. Starting concurrent parsing for {LIMIT_BUSES} buses."
            )

        except Exception as e:
            log.critical(f"Initial setup/network failure. Cannot run tests: {e}")
            console.print(
                Panel(
                    f"[bold red]CRITICAL SETUP FAILURE:[/bold red] {e}",
                    title="Test Aborted",
                )
            )
            return

        parser_tasks = {
            name: run_parser(name, client, initial_html, LIMIT_BUSES, write_lock)
            for name in PARSERS_MAP.keys()
        }

        all_results = await asyncio.gather(
            *parser_tasks.values(), return_exceptions=False
        )
        all_results = dict(zip(parser_tasks.keys(), all_results))

    summary = get_comparison_summary(all_results)

    console.rule("[bold yellow]Parser Consistency Check Results[/bold yellow]")
    log.info(f"Consistency check completed for {len(summary)} bus services.")

    overall_consistent_count = 0

    for bus in summary:
        ref_trip = bus["reference_trip_code"]
        bs_service = bus["bs_service"]

        if bs_service is None:
            console.print(
                Panel(
                    f"[red]Bus {bus['index']} (Ref: N/A):[/red] BeautifulSoup failed to extract this service.",
                    style="bold red",
                )
            )
            log.warning(f"Bus {bus['index']}: BS service is None.")
            continue

        bus_table = Table(
            title=f"[bold cyan]Bus #{bus['index'] + 1}[/bold cyan] | Trip Code: [bold green]{ref_trip}[/bold green] | Type: {bs_service.bus_type}",
            show_header=True,
            header_style="bold magenta",
            show_footer=False,
            box=None,
        )
        bus_table.add_column("Field", style="bold yellow")
        bus_table.add_column("BS Reference", style="bold white")
        bus_table.add_column("Comparison Details", justify="left", min_width=50)

        is_bus_fully_consistent = True

        all_fields = list(BusService.model_fields.keys())

        for field in all_fields:
            if field in ["llm_reasoning", "explanation"]:
                continue

            bs_val = getattr(bs_service, field, None)

            gemini_comp = bus["comparisons"].get("gemini", {})
            ollama_comp = bus["comparisons"].get("ollama", {})

            gemini_diffs = gemini_comp.get("diffs", {})
            ollama_diffs = ollama_comp.get("diffs", {})

            gemini_service = gemini_comp.get("service")
            ollama_service = ollama_comp.get("service")

            gemini_val = (
                getattr(gemini_service, field, "N/A (Parse Fail)")
                if gemini_service
                else "N/A (No Service)"
            )
            ollama_val = (
                getattr(ollama_service, field, "N/A (Parse Fail)")
                if ollama_service
                else "N/A (No Service)"
            )

            has_diff = field in gemini_diffs or field in ollama_diffs

            if has_diff:
                is_bus_fully_consistent = False
                field_style = "bold red" if field in CRITICAL_FIELDS else "bold orange3"

                diff_table = Table(box=None, show_header=False, padding=(0, 1))
                diff_table.add_column("Parser", style="dim", justify="right")
                diff_table.add_column("Reported Value", justify="left")

                g_style = "red" if field in gemini_diffs else "green"
                diff_table.add_row(
                    Text("Gemini", style=g_style), Text(str(gemini_val), style=g_style)
                )

                o_style = "red" if field in ollama_diffs else "green"
                diff_table.add_row(
                    Text("Ollama", style=o_style), Text(str(ollama_val), style=o_style)
                )

                comparison_content = diff_table
            else:
                field_style = "bold white"

                comparison_content = Text(str(gemini_val), style="green")

            bus_table.add_row(
                Text(field, style=field_style), str(bs_val), comparison_content
            )

        console.print(Panel(bus_table, border_style="bold cyan"))

        if is_bus_fully_consistent:
            overall_consistent_count += 1
            console.print(
                Text(
                    f"--> Bus {ref_trip}: Fully consistent across all parsers.",
                    style="bold green",
                )
            )
            log.info(f"Bus {ref_trip}: Fully consistent.")
        else:
            console.print(
                Text(
                    f"--> Bus {ref_trip}: Differences found (highlighted in red).",
                    style="bold red",
                )
            )
            log.warning(
                f"Bus {ref_trip}: Inconsistent results detected. Diffs: {bus['comparisons']}"
            )

        console.print("\n")

    final_panel_style = (
        "bold green" if overall_consistent_count == len(summary) else "bold yellow"
    )
    console.print(
        Panel(
            f"Processed {len(summary)} services.\n"
            f"Services with full consistency: [b]{overall_consistent_count}[/b] / {len(summary)}",
            title="[bold blue]Overall Test Summary[/bold blue]",
            style=final_panel_style,
        )
    )
    log.info(
        f"Overall Test Summary: {overall_consistent_count} / {len(summary)} services fully consistent."
    )


if __name__ == "__main__":
    asyncio.run(main_test_runner())
