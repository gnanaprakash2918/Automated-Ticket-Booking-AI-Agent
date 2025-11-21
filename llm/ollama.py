import os
from typing import Any, Optional, Type
from loguru import logger
import ollama
from llm.interface import LLMInterface, T


class OllamaLLM(LLMInterface):
    def __init__(
        self,
        model_name: Optional[str] = None,
        base_url_env: str = "OLLAMA_BASE_URL",
        prompt_dir: str = "prompts",
    ):
        super().__init__(prompt_dir=prompt_dir)

        self.model = model_name or os.getenv(
            "OLLAMA_MODEL", "mistral-nemo:12b-instruct-2407-q4_K_M"
        )

        base_url = os.getenv(base_url_env, "http://localhost:11434")

        logger.debug(
            f"Initializing Ollama client with model={self.model}, base_url={base_url}"
        )

        try:
            self.client = ollama.AsyncClient(host=base_url)
            logger.debug("Ollama AsyncClient initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to construct Ollama AsyncClient: {e}")
            raise RuntimeError(f"Failed to construct Ollama AsyncClient: {e}") from e

    async def generate(self, prompt: str, **kwargs: Any) -> str:
        debug_prompt = prompt[:100] + "..." if len(prompt) > 100 else prompt
        logger.debug(f"Ollama generating text for prompt: {debug_prompt}")

        try:
            resp = await self.client.chat(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                options={"temperature": kwargs.get("temperature", 0.0)},
            )

            content = resp.get("message", {}).get("content")
            return str(content) if content else ""
        except Exception as e:
            logger.error(f"Ollama generation failed: {e}")
            raise RuntimeError(f"Ollama generation failed: {e}") from e

    async def generate_structured(
        self, schema: Type[T], prompt: str, system_prompt: str = "", **kwargs: Any
    ) -> T:
        logger.debug("Ollama structured generation requested.")

        full_prompt = (
            f"{system_prompt}\nUser Request:\n{prompt}" if system_prompt else prompt
        )

        try:
            response_text = await self.generate(full_prompt, **kwargs)
            return self.parse_json_response(response_text, schema)

        except Exception as e:
            logger.error(f"Ollama structured generation failed: {e}")
            raise RuntimeError(f"Ollama structured generation failed: {e}") from e
