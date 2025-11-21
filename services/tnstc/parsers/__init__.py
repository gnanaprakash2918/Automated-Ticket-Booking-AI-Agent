from typing import Optional, cast
from loguru import logger
from ..config import PARSER_STRATEGY
from .base import AbstractBusParser as BusParser
from .bs_parser import BeautifulSoupParser
from .gemini_parser import GeminiParser
from .ollama_parser import OllamaParser


_parser_instance: Optional[BusParser] = None


def get_parser() -> BusParser:
    """
    Factory function to get the configured bus parser instance.
    Reads the PARSER_STRATEGY from config and returns the singleton instance.
    """
    global _parser_instance

    if _parser_instance is not None:
        current_strategy = PARSER_STRATEGY
        # Check if strategy matches the instance type
        if (
            (isinstance(_parser_instance, GeminiParser) and current_strategy == "gemini")
            or (isinstance(_parser_instance, BeautifulSoupParser) and current_strategy == "beautifulsoup")
            or (isinstance(_parser_instance, OllamaParser) and current_strategy == "ollama")
        ):
            return cast(BusParser, _parser_instance)
        else:
            logger.info(f"Parser strategy changed to '{current_strategy}'. Re-initializing parser...")

    _parser_instance = None

    if PARSER_STRATEGY == "gemini":
        logger.info("Initializing GeminiParser.")
        try:
            _parser_instance = GeminiParser()
        except ValueError as e:
            logger.error(f"Failed to initialize GeminiParser: {e}. Defaulting to 'beautifulsoup'.")
            _parser_instance = BeautifulSoupParser()

    elif PARSER_STRATEGY == "ollama":
        logger.info("Initializing OllamaParser.")
        try:
            _parser_instance = OllamaParser()
        except Exception as e:
            logger.error(f"Failed to initialize OllamaParser: {e}. Defaulting to 'beautifulsoup'.")
            _parser_instance = BeautifulSoupParser()

    elif PARSER_STRATEGY == "beautifulsoup":
        logger.info("Initializing BeautifulSoupParser.")
        _parser_instance = BeautifulSoupParser()

    else:
        logger.warning(f"Invalid PARSER_STRATEGY: '{PARSER_STRATEGY}'. Defaulting to 'beautifulsoup'.")
        _parser_instance = BeautifulSoupParser()

    return cast(BusParser, _parser_instance)
