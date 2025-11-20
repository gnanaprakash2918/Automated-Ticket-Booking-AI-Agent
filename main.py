import asyncio
from datetime import datetime
import importlib
from typing import Any, Optional
from fastapi import Body, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from pydantic import ValidationError
import uvicorn
from utils.logger import setup_logging


def create_app(service_name: str = "tnstc"):
    setup_logging()
    logger.info(f"Creating FastAPI app for service '{service_name}'")

    service_instance = None

    try:
        config_module = importlib.import_module(f"services.{service_name}.config")
        service_module = importlib.import_module(f"services.{service_name}.service")
        schemas_module = importlib.import_module(f"services.{service_name}.schemas")

        logger.debug(
            f"Modules loaded for service '{service_name}': "
            f"{config_module}, {service_module}, {schemas_module}"
        )

        ServiceClass = getattr(service_module, f"{service_name.upper()}Service")
        RequestSchema = getattr(schemas_module, f"{service_name.upper()}SearchRequest")
        ResponseSchema = getattr(
            schemas_module, f"{service_name.upper()}BusSearchResponse"
        )
        MetaSchema = getattr(schemas_module, f"{service_name.upper()}ResponseMetadata")

        PARSER_STRATEGY = getattr(config_module, "PARSER_STRATEGY", "dynamic")
        GEMINI_MODEL = getattr(config_module, "GEMINI_MODEL", None)
        OLLAMA_MODEL = getattr(config_module, "OLLAMA_MODEL", None)
        APP_ENV = getattr(config_module, "APP_ENV", None)

        service_instance = ServiceClass()
        logger.info(f"Service instance for '{service_name}' created successfully.")

    except Exception as e:
        logger.critical(f"Failed to load service modules for '{service_name}': {e}")
        raise RuntimeError("Service Loading Failed")

    async def startup_event():
        logger.info(f"Running startup event for service '{service_name}'")
        if hasattr(service_instance, "initialize_db"):
            await service_instance.initialize_db()
            logger.info("Database initialized successfully during startup.")

        logger.info(f"Service '{service_name}' lifecycle started.")

    app = FastAPI(
        title=f"{service_name.upper()} API",
        version="2.2.0",
        on_startup=[startup_event],
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health", tags=["Health"])
    async def health():
        logger.info("Health endpoint hit.")
        return {"status": "ok", "timestamp": datetime.now()}

    @app.get("/config_info", tags=["Diagnostics"])
    async def get_config_info():
        logger.info("Config info endpoint hit.")
        return {
            "parser_strategy": PARSER_STRATEGY,
            "gemini_model": GEMINI_MODEL,
            "ollama_model": OLLAMA_MODEL,
            "app_env": APP_ENV,
        }

    @app.post("/search", response_model=ResponseSchema)
    async def search(
        payload: Any = Body(..., description="Search request payload"),
        limit: Optional[int] = Query(
            None,
            gt=0,
            title="Limit Parsed Results",
            description="Process and return only the first 'n' bus services found.",
        ),
    ):
        start_time = datetime.now()
        logger.info(f"/search endpoint hit at {start_time.isoformat()}")

        try:
            req = RequestSchema.parse_obj(payload)
            logger.info(
                f"Received search request: {req.from_place_name} -> "
                f"{req.to_place_name} on {req.onward_date}"
            )

        except ValidationError as ve:
            logger.error(f"Validation Error: {ve}")
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY, detail=ve.errors()
            )

        try:
            from_place = None
            to_place = None

            if hasattr(service_instance, "_fetch_place_info"):
                logger.debug(
                    "Resolving from/to places via service_instance._fetch_place_info"
                )

                from_task = service_instance._fetch_place_info(
                    req.from_place_name, is_from_place=True
                )

                to_task = service_instance._fetch_place_info(
                    req.to_place_name, is_from_place=False
                )

                from_place, to_place = await asyncio.gather(from_task, to_task)

                logger.debug(
                    "Resolved places -> "
                    f"From={getattr(from_place, 'code', None)}, "
                    f"To={getattr(to_place, 'code', None)}"
                )

            services = await service_instance.search_services(req)
            logger.info(f"Service returned {len(services)} services before limit.")

            if limit and services:
                logger.debug(f"Applying limit={limit} to services list.")
                services = services[:limit]

            if not services:
                logger.warning("No bus services found for given criteria.")
                raise HTTPException(status.HTTP_404_NOT_FOUND, "No buses found")

            parser_strategy_value = "dynamic"

            meta = MetaSchema(
                search_timestamp=start_time,
                parser_strategy=parser_strategy_value,
                total_services_found_before_filtering=len(services),
                limit_applied=limit,
            )

            if from_place is not None:
                from_payload = from_place
            else:
                from_payload = {
                    "id": "000",
                    "code": "UNK",
                    "name": req.from_place_name,
                }

            if to_place is not None:
                to_payload = to_place
            else:
                to_payload = {
                    "id": "000",
                    "code": "UNK",
                    "name": req.to_place_name,
                }

            logger.info(
                f"Search completed successfully, returning {len(services)} services."
            )

            return ResponseSchema(
                from_place=from_payload,
                to_place=to_payload,
                services=services,
                metadata=meta,
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Unhandled error in search endpoint")
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, str(e))

    return app


if __name__ == "__main__":
    app = create_app("tnstc")
    uvicorn.run(app, host="0.0.0.0", port=9000)
