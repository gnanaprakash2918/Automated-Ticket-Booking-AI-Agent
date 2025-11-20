from abc import ABC, abstractmethod
import json
import os
from typing import Any, Type, TypeVar
from venv import logger
from pydantic import BaseModel


T = TypeVar("T", bound=BaseModel)


class LLMInterface(ABC):
    """
    Base class that combines:

    1. Abstract methods for Generation (interface).
    2. Concrete methods for Prompt Loading.
    """

    def __init__(self, prompt_dir: str = "prompts"):
        """
        Initialize the LLM with a directory to look for prompt overrides.
        """

        self.prompt_dir = prompt_dir
        if not os.path.exists(self.prompt_dir):
            logger.debug(
                f"Prompt directory '{self.prompt_dir}' does not exist. Creating it."
            )

            try:
                os.makedirs(self.prompt_dir, exist_ok=True)
                logger.debug(f"Prompt directory '{self.prompt_dir}' created.")
            except OSError:
                logger.error(f"Failed to create prompt directory '{self.prompt_dir}'.")
                raise

    def load_prompt(self, filename: str) -> str:
        """
        Loads a prompt template File in 'prompts/' directory
        """

        file_path = os.path.join(self.prompt_dir, filename)
        if os.path.exists(file_path):
            logger.debug(f"Loading prompt from '{file_path}'")
            try:
                logger.debug(f"Loading prompt from '{file_path}'")
                with open(file_path, "r", encoding="utf-8") as f:
                    return f.read()
            except Exception:
                logger.error(f"Failed to load prompt from '{file_path}'.")
                raise

        raise FileNotFoundError(
            f"Prompt '{filename}' not found on disk or in defaults."
        )

    def construct_system_prompt(
        self, schema: Type[BaseModel], filename: str = "system_prompt.txt"
    ) -> str:
        """
        Helper: Loads system prompt template and injects JSON schema.
        """

        raw_prompt = self.load_prompt(filename)
        schema_json = json.dumps(schema.model_json_schema(), indent=2)

        if "{{JSON_SCHEMA}}" in raw_prompt:
            return raw_prompt.replace("{{JSON_SCHEMA}}", schema_json)

        return f"{raw_prompt}\n\n## JSON Output Schema\n{schema_json}"

    def construct_user_prompt(
        self, main_html: str, detail_html: str, filename: str = "user_prompt.txt"
    ) -> str:
        """
        Helper: Loads user prompt template and injects HTML/Few-shot data.
        """

        template = self.load_prompt(filename)

        try:
            few_shot = self.load_prompt("few_shot_examples.txt")
        except FileNotFoundError:
            few_shot = ""

        content = template.replace("{{FEW_SHOT}}", few_shot)
        content = content.replace("{{MAIN_HTML}}", main_html)
        content = content.replace("{{DETAIL_HTML}}", detail_html)
        return content

    @abstractmethod
    async def generate(self, prompt: str, **kwargs: Any) -> str:
        """Generate simple text."""
        raise NotImplementedError

    @abstractmethod
    async def generate_structured(
        self, schema: Type[T], prompt: str, system_prompt: str = "", **kwargs: Any
    ) -> T:
        """Generate structured Pydantic object."""
        raise NotImplementedError
