import asyncio
from datetime import datetime
import importlib
import logging
from typing import Any, Optional
from fastapi import Body, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError
import uvicorn
from utils.logger import setup_logging


def main(service_name: str):
    """
    Creates and configures a FastAPI application for a specific service.
    """
    setup_logging()
    log = logging.getLogger(__name__)

    try:
        config_module = importlib.import_module(f"services.{service_name}.config")
        schemas_module = importlib.import_module(f"services.{service_name}.schemas")
        service_module = importlib.import_module(f"services.{service_name}.service")
    except ImportError as e:
        log.error(f"Error importing modules for service '{service_name}': {e}")
        raise

    ServiceClass = getattr(service_module, f"{service_name.upper()}Service")
    SearchRequestSchema = getattr(
        schemas_module, f"{service_name.upper()}SearchRequest"
    )
    BusSearchResponseSchema = getattr(
        schemas_module, f"{service_name.upper()}BusSearchResponse"
    )
    ResponseMetadataSchema = getattr(
        schemas_module, f"{service_name.upper()}ResponseMetadata"
    )

    PARSER_STRATEGY = getattr(config_module, "PARSER_STRATEGY")
    GEMINI_MODEL = getattr(config_module, "GEMINI_MODEL")
    OLLAMA_MODEL = getattr(config_module, "OLLAMA_MODEL")
    APP_ENV = getattr(config_module, "APP_ENV")

    # Initialize FastAPI App
    app = FastAPI(
        title=f"{service_name.upper()} API Wrapper",
        description=f"A FastAPI wrapper for the {service_name.upper()} booking website",
        version="1.0.0",
    )

    # For development - adjust as you need. Removed "*" when allow_credentials=True.
    DEVELOPMENT_ORIGINS = ["http://localhost:9000", "http://127.0.0.1:9000"]

    app.add_middleware(
        CORSMiddleware,
        allow_credentials=True,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
        allow_origins=DEVELOPMENT_ORIGINS,
    )

    service_instance = ServiceClass()

    # Endpoints
    @app.get("/", tags=["Health"])
    async def check_health():
        log.info("Health Check Endpoint was hit.")
        return {
            "status": "ok",
            "message": f"{service_name.upper()} API Wrapper is running.",
        }

    @app.get("/config_info", tags=["Diagnostics"])
    async def get_config_info():
        """Returns non-sensitive runtime configuration."""
        return {
            "parser_strategy": PARSER_STRATEGY,
            "gemini_model": GEMINI_MODEL,
            "ollama_model": OLLAMA_MODEL,
            "app_env": APP_ENV,
        }

    @app.post(
        "/search_buses",
        response_model=BusSearchResponseSchema,
        status_code=status.HTTP_200_OK,
    )
    async def search_buses(
        request: Any = Body(
            ..., description="Search request payload (validated at runtime)"
        ),
        limit: Optional[int] = Query(
            default=None,
            gt=0,
            title="Limit Parsed Results",
            description="Process and return only the first 'n' bus services found.",
        ),
    ):
        """
        Performs the full, multi-step bus search against the external API, and then filters the results.
        We accept the raw JSON body and parse it with the dynamically-loaded Pydantic model.
        """
        search_time = datetime.now()

        try:
            parsed_request = SearchRequestSchema.parse_obj(request)
        except ValidationError as ve:
            log.warning("Request validation failed: %s", ve)
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={"validation_error": ve.errors()},
            )

        log.info(
            f"Received search request: {parsed_request.from_place_name} -> "
            f"{parsed_request.to_place_name} on {parsed_request.onward_date}"
        )

        try:
            from_place_task = service_instance._fetch_place_info(
                parsed_request.from_place_name, is_from_place=True
            )
            to_place_task = service_instance._fetch_place_info(
                parsed_request.to_place_name, is_from_place=False
            )
            from_place, to_place = await asyncio.gather(from_place_task, to_place_task)

            services = await service_instance.search_services(parsed_request)

            if not services:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No bus services found matching the specified route, date, and filters.",
                )

            total_found = len(services)
            log.info(
                f"Bus parsing complete. Parser found {total_found} services (before filtering)."
            )

            filtered_bus_list = service_instance._filter_bus_services(
                services, parsed_request
            )

            if not filtered_bus_list:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No bus services found matching the specified route, date, and filters.",
                )

            log.info(
                f"Filtering complete. {len(filtered_bus_list)} services remain after applying filters."
            )

            metadata_obj = ResponseMetadataSchema(
                search_timestamp=search_time,
                parser_strategy=PARSER_STRATEGY,
                total_services_found_before_filtering=total_found,
                limit_applied=limit,
            )

            return BusSearchResponseSchema(
                metadata=metadata_obj,
                from_place=from_place,
                to_place=to_place,
                services=filtered_bus_list,
            )

        except HTTPException:
            raise
        except Exception as e:
            log.exception("Unexpected error during search")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Unexpected error during search: {e}",
            )

    return app


if __name__ == "__main__":
    service_to_run = "tnstc"
    app = main(service_to_run)
    uvicorn.run(app, host="localhost", port=9000, reload=False)
