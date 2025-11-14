import json
from pydantic import BaseModel
from typing import Type, Any
from ..schemas import *
from pydantic import BaseModel
from typing import Type, Any, get_args
import json
import json
import inspect

def _get_base_type(type_hint: Any) -> Any:
    """Recursively resolves the inner type from complex type hints (e.g., Optional, List)."""
    args = get_args(type_hint)
    if args:
        return _get_base_type(args[0])
    return type_hint

def extract_examples(model: Type[BaseModel]) -> str:
    """
    Recursively extracts and formats ALL JSON examples from a Pydantic model 
    and its nested models.
    """
    example_str = "## Reference Examples for Data Formatting\n"
    
    if 'json_schema_extra' in model.model_config and isinstance(model.model_config['json_schema_extra'], dict) and 'examples' in model.model_config['json_schema_extra']:
        examples = model.model_config['json_schema_extra']['examples']
        
        if examples:
            example_str += f"- **{model.__name__} Examples:**\n"
            
            for i, example in enumerate(examples): # type: ignore
                example_str += f"  - **Example {i + 1}:**\n"
                example_str += "```json\n" + json.dumps(example, indent=2) + "\n```\n"

    for name, field in model.model_fields.items():
        field_type = _get_base_type(field.annotation)
        
        if inspect.isclass(field_type) and issubclass(field_type, BaseModel) and field_type != model:
            nested_examples = extract_examples(field_type)
            example_str += nested_examples.replace("## Reference Examples for Data Formatting", "").strip()

    return example_str.strip()

class PromptGenerator:
    """
    Generates a high-quality, human-readable system prompt for an LLM
    based on a Pydantic model's schema.
    
    This prompt is designed for structured JSON extraction and is far
    more effective than simply dumping the raw JSON schema.
    """

    def build_system_prompt(self, pydantic_model: Type[BaseModel]) -> str:
        """
        Builds the main system prompt for the given Pydantic model.
        """
        json_schema = BusSearchResponse.model_json_schema()    
        examples_hint = extract_examples(BusSearchResponse)
        
        system_content = f"""
        You are a reliable JSON generation engine and an expert automated HTML parsing engine.\n
        Your entire output MUST be a single, valid JSON object that strictly conforms to the provided JSON Schema.\n
        DO NOT include any conversational text or markdown outside of the final JSON block.\n

        {examples_hint}

        ## JSON Output Schema (Strict Constraint)
        {json.dumps(json_schema, indent=2)}
        """

        return system_content