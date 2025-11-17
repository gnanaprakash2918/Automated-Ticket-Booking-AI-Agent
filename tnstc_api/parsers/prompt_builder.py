import json
from pydantic import BaseModel
from typing import Type, Any
from ..schemas import *
from pydantic import BaseModel
from typing import Type, Any, get_args
import json
import json
import inspect
from utils.clean_html import minify_html

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
    """

    def build_system_prompt(self, pydantic_model: Type[BaseModel]) -> str:
        """
        Builds the main system prompt for the given Pydantic model.
        """
        
        json_schema = pydantic_model.model_json_schema() 
        examples_hint = extract_examples(pydantic_model)
        
        system_content = f"""
        You are a reliable JSON generation engine and an expert automated HTML parsing engine.
        Your entire output MUST be a single, valid JSON object that strictly conforms to the provided JSON Schema.
        DO NOT include any conversational text or markdown outside of the final JSON block.

        {examples_hint}

        ## JSON Output Schema (Strict Constraint)
        {json.dumps(json_schema, indent=2)}
        """

        return system_content

    def _build_few_shot_examples(self) -> str:
        """
        Creates a string of examples to guide the LLM.
        Easy to extend - just add dicts to the examples list.
        """
        
        examples = [
            {
                "main_html": """<div class="bus-list" data-bus-type="AC 3X2" data-time="00:30"><div class="bus-item">...[FULL HTML]...</div></div>""",
                "detail_html": """<html><body><form><table>...[FULL HTML]...</table></form></body></html>""",
                "json_output": {
                    "operator": "SALEM",
                    "bus_type": "AC 3X2",
                    "trip_code": "0030SALBANDD02A",
                    "route_code": "100J",
                    "departure_time": "00:30",
                    "arrival_time": "05:30",
                    "duration": "5.00",
                    "price_in_rs": 269,
                    "seats_available": 41,
                    "via_route": ["HOSUR"],
                    "total_kms": "208.00",
                    "child_fare": "NA"
                }
            },
            {
                "main_html": """<div class="bus-list" data-bus-type="AC 3X2" data-time="00:30"><div class="bus-item">...[FULL HTML]...</div></div>""",
                "detail_html": """<html><body><form><table>...[FULL HTML]...</table></form></body></html>""",
                "json_output": {
                    "operator": "SALEM",
                    "bus_type": "DELUXE 3X2",
                    "trip_code": "0100SALBANDD01L",
                    "route_code": "100A",
                    "departure_time": "01:00",
                    "arrival_time": "06:00",
                    "duration": "5.00",
                    "price_in_rs": 229,
                    "seats_available": 39,
                    "via_route": ["HOSUR"],
                    "total_kms": "208.00",
                    "child_fare": "NA"
                }
            },
            {
                "main_html": """<div class="bus-list" data-bus-type="DELUXE 3X2" data-time="01:00"><div class="bus-item">...[FULL HTML]...</div></div>""",
                "detail_html": """<html><body><form><table>...[FULL HTML]...</table></form></body></html>""",
                "json_output": {
                    "operator": "SALEM",
                    "bus_type": "DELUXE 3X2",
                    "trip_code": "0300SALBANDD01L",
                    "route_code": "100L",
                    "departure_time": "03:00",
                    "arrival_time": "08:00",
                    "duration": "5.00",
                    "price_in_rs": 229,
                    "seats_available": 38,
                    "via_route": ["HOSUR"],
                    "total_kms": "208.00",
                    "child_fare": "NA"
                }
            }
        ]
        
        prompt_parts = ["Here are some examples of how to extract the data:\n"]
        
        for idx, example in enumerate(examples, 1):
            prompt_parts.append(f"""
            ---
            EXAMPLE {idx}
            ---
            MAIN_LIST_HTML
            {minify_html(example["main_html"])}
            ---
            DETAIL_TABLE_HTML
            {minify_html(example["detail_html"])}
            ---
            CORRECT JSON OUTPUT
            {json.dumps(example["json_output"], indent=2)}
            ---
            END EXAMPLE {idx}
            ---
            """)
        
        prompt_parts.append("\nNow, perform the same task on the new data provided below.")
        return "".join(prompt_parts)
