import json
from pydantic import BaseModel
from typing import Type, List, Dict, Any

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
        try:
            # Get the schema from the model
            schema = pydantic_model.model_json_schema()
        except Exception as e:
            print(f"Error generating schema: {e}")
            return "Error: Could not generate model schema."
            
        model_name = schema.get('title', pydantic_model.__name__)
        model_desc = schema.get('description', 'No model description provided.')
        
        # Build the different parts of the prompt
        field_rules = self._build_field_rules(schema)
        examples = self._build_examples(schema)
        
        # Assemble the final prompt
        prompt = f"""
You are a high-precision JSON extraction assistant. Your task is to analyze the
provided HTML context and extract data to populate a *single*, valid JSON object.

You MUST adhere to the following strict rules:
1.  **JSON ONLY**: Your entire response MUST be a single, raw JSON object. Do not
    include any text, explanations, comments, or markdown formatting (like ```json).
2.  **STRICT SCHEMA**: The JSON object MUST conform *exactly* to the target
    model: **{model_name}**.
3.  **NULL FOR MISSING**: If a field's value cannot be found in the text, you
    MUST use `null` (unless it's a list, then use `[]`).
4.  **NO HALLUCINATIONS**: Do not invent data. Only extract data present in the
    provided HTML.
5.  **FOLLOW ALL RULES**: Pay close attention to the specific data types,
    formats, and validation rules listed for each field below.

---
### Target Model: {model_name}
{model_desc}

---
### Field-by-Field Rules & Validation
You must follow these rules for each field:

{field_rules}

---
### Output Examples
Here are one or more examples of the *exact* JSON output format you must produce.

{examples}

---
### FINAL TASK
Analyze the user's HTML snippets. Extract all available data according to the
field rules. Return *ONLY* the single, raw, valid JSON object.
"""
        return prompt

    def _build_field_rules(self, schema: Dict[str, Any]) -> str:
        """Helper to build the human-readable list of field rules."""
        rules: List[str] = []
        properties = schema.get('properties', {})
        required_fields = set(schema.get('required', []))

        for field_name, details in properties.items():
            # 1. Get Type
            field_type = details.get('type', 'any')
            if field_type == 'array':
                item_type = details.get('items', {}).get('type', 'any')
                field_type = f"List[ {item_type} ]"
            
            # 2. Get Requirement
            req_status = "REQUIRED" if field_name in required_fields else "OPTIONAL"
            
            # 3. Get Description (This is the most important part)
            desc = details.get('description', 'No description.')
            
            # 4. Get Default
            default_val = details.get('default')
            default_str = f" (Default: {json.dumps(default_val)})" if default_val is not None else ""

            # 5. Assemble
            rules.append(
                f"- **{field_name}** (`{field_type}` | *{req_status}*): {desc}{default_str}"
            )
        
        if not rules:
            return "No fields defined in schema."
            
        return "\n".join(rules)

    def _build_examples(self, schema: Dict[str, Any]) -> str:
        """Helper to extract and format examples from the schema."""
        
        # Pydantic v2 stores examples in 'json_schema_extra'
        examples = schema.get('json_schema_extra', {}).get('examples', [])
        
        # Fallback for older Pydantic or direct 'examples' key
        if not examples and 'examples' in schema:
            examples = schema['examples']
            
        if not examples:
            return "No examples provided in the schema."

        example_strs: List[str] = []
        for i, ex in enumerate(examples):
            example_strs.append(
                f"**Example {i + 1}**:\n```json\n{json.dumps(ex, indent=2)}\n```"
            )
            
        return "\n\n".join(example_strs)