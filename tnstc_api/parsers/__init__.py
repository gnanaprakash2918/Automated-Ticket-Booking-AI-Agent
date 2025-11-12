import logging
from ..config import PARSER_STRATEGY
from .base import BusParser
from .bs_parser import BeautifulSoupParser
from .gemini_parser import GeminiParser
from .ollama_parser import OllamaParser

log = logging.getLogger(__name__)

_parser_instance: BusParser = None # type: ignore

def get_parser() -> BusParser:
    """
    Factory function to get the configured bus parser instance.
    
    Reads the PARSER_STRATEGY from config and returns the
    appropriate singleton parser instance.
    """

    global _parser_instance
    
    if _parser_instance is not None:
        current_strategy = PARSER_STRATEGY
        if (isinstance(_parser_instance, GeminiParser) and current_strategy == "gemini") or \
           (isinstance(_parser_instance, BeautifulSoupParser) and current_strategy == "beautifulsoup") or \
           (isinstance(_parser_instance, OllamaParser) and current_strategy == "ollama"):
            return _parser_instance
        else:
            log.info(f"Parser strategy changed. Re-initializing parser...")

    _parser_instance = None # type: ignore

    if PARSER_STRATEGY == "gemini":
        log.info("Initializing GeminiParser.")
        try:
            _parser_instance = GeminiParser()
        except ValueError as e:
            log.error(f"Failed to initialize GeminiParser: {e}. Defaulting to 'beautifulsoup'.")
            _parser_instance = BeautifulSoupParser()
    
    elif PARSER_STRATEGY == "ollama":
        log.info("Initializing OllamaParser.")
        try:
            _parser_instance = OllamaParser()
        except Exception as e:
            log.error(f"Failed to initialize OllamaParser: {e}. Defaulting to 'beautifulsoup'.")
            _parser_instance = BeautifulSoupParser()

    elif PARSER_STRATEGY == "beautifulsoup":
        log.info("Initializing BeautifulSoupParser.")
        _parser_instance = BeautifulSoupParser()
        
    else:
        log.error(f"Invalid PARSER_STRATEGY: '{PARSER_STRATEGY}'. Defaulting to 'beautifulsoup'.")
        _parser_instance = BeautifulSoupParser()

    return _parser_instance