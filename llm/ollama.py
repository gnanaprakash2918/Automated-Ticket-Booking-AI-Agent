import os
from typing import Type, Optional, Any, cast
import ollama

from llm.interface import LLMInterface, T

class OllamaLLM(LLMInterface):
    """
    Async Ollama adapter.
    """

    def __init__(
        self, 
        model_name: Optional[str] = None, 
        base_url_env: str = "OLLAMA_BASE_URL",
        prompt_dir: str = "prompts"
    ):
        super().__init__(prompt_dir=prompt_dir)

        self.model = model_name or os.getenv("OLLAMA_MODEL", "llama3")
        base_url = os.getenv(base_url_env, "http://localhost:11434")

        try:
            self.client = ollama.AsyncClient(host=base_url)
        except Exception as e:
            raise RuntimeError(f"Failed to construct Ollama AsyncClient: {e}") from e

    async def generate(self, prompt: str, **kwargs: Any) -> str:
        try:
            response = await self.client.chat(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                options={"temperature": kwargs.get("temperature", 0.0)}
            )
            
            content = response.get("message", {}).get("content")
            return str(content) if content else ""
        except Exception as e:
            raise RuntimeError(f"Ollama generation failed: {e}") from e

    async def generate_structured(
        self, 
        schema: Type[T], 
        prompt: str, 
        system_prompt: str = "", 
        **kwargs: Any
    ) -> T:
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})

        messages.append({"role": "user", "content": prompt})

        try:
            response = await self.client.chat(
                model=self.model,
                messages=messages,
                format="json",
                options={"temperature": kwargs.get("temperature", 0.0)}
            )

            json_content = response.get("message", {}).get("content")

            if isinstance(json_content, (dict, list)):
                return cast(T, schema.model_validate(json_content))
            
            if isinstance(json_content, str):
                clean_json = json_content.strip().strip("`").replace("json\n", "")
                return cast(T, schema.model_validate_json(clean_json))
            
            raise ValueError("Invalid content received from Ollama")
        
        except Exception as e:
            raise RuntimeError(f"Ollama structured generation failed: {e}") from e