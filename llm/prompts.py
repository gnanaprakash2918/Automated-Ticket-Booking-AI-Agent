import json
import os
from typing import Type
from pydantic import BaseModel

class PromptLoader:
    """
    Handles file I/O for prompt templates.
    """

    def __init__(self, base_dir: str = "prompts"):
        self.base_dir = base_dir

    def _resolve(self, filename: str) -> str:
        if not os.path.isabs(filename):
            return os.path.join(self.base_dir, filename)
        return filename

    def load(self, filename: str) -> str:
        path = self._resolve(filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Prompt file not found at: {path}")
        
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

class PromptGenerator:
    """
    Centralized prompt construction. 
    Parsers call methods here to get the final strings they need.
    """

    def __init__(self, base_dir: str = "prompts"):
        self.loader = PromptLoader(base_dir)
        self.few_shot_content = self.loader.load("few_shot_examples.txt")

    def build_system_prompt(self, pydantic_model: Type[BaseModel], filename: str = "system_prompt.txt") -> str:
        """
        Builds the system instruction string with the JSON schema injected.
        """

        raw = self.loader.load(filename)
        schema_json = json.dumps(pydantic_model.model_json_schema(), indent=2)
        
        if "{{JSON_SCHEMA}}" in raw:
            return raw.replace("{{JSON_SCHEMA}}", schema_json)
        return f"{raw}\n\n## JSON Output Schema\n{schema_json}"

    def build_user_message(self, main_html: str, detail_html: str, filename: str = "user_prompt.txt") -> str:
        """
        Constructs the final user message by combining:
        1. The user_prompt template
        2. The few-shot examples (injected into template)
        3. The dynamic HTML inputs (injected into template)
        """
        
        template = self.loader.load(filename)
        
        content = template.replace("{{FEW_SHOT}}", self.few_shot_content)
        content = content.replace("{{MAIN_HTML}}", main_html)
        content = content.replace("{{DETAIL_HTML}}", detail_html)
        
        return content