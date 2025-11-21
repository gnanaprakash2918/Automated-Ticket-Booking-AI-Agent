import os
from typing import Any, Optional, Type, cast
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from loguru import logger
from llm.interface import LLMInterface, T


class GeminiLLM(LLMInterface):
    def __init__(
        self,
        model_name: Optional[str] = None,
        api_key_env: str = "GEMINI_API_KEY",
        prompt_dir: str = "prompts",
        timeout: int = 60,
    ):
        logger.debug("Initializing Gemini LLM Interface")
        super().__init__(prompt_dir=prompt_dir)

        model = model_name or os.getenv("GEMINI_MODEL", "gemini-2.5-pro")
        api_key = os.getenv(api_key_env) or os.getenv("GOOGLE_API_KEY")

        logger.debug(f"Using Gemini model: {model}")
        if not api_key:
            logger.error("Gemini API key not set.")
            raise ValueError("Gemini API key not set.")

        logger.debug("Gemini API key set successfully.")
        self.llm = ChatGoogleGenerativeAI(
            model=model, api_key=api_key, request_timeout=timeout, temperature=0.0
        )

    async def generate(self, prompt: str, **kwargs: Any) -> str:
        try:
            debug_prompt = prompt[:100] + "..." if len(prompt) > 100 else prompt
            logger.debug(f"Generating text with prompt: {debug_prompt}")

            response = await self.llm.ainvoke(prompt)
            content = getattr(response, "content", str(response))

            logger.debug("Generated content using Gemini")
            return str(content)

        except Exception as e:
            raise RuntimeError(f"Gemini generation failed: {e}") from e

    async def generate_structured(
        self, schema: Type[T], prompt: str, system_prompt: str = "", **kwargs: Any
    ) -> T:
        try:
            debug_prompt = prompt[:100] + "..." if len(prompt) > 100 else prompt
            logger.debug(f"Generating structured output with prompt: {debug_prompt}")

            structured_llm = self.llm.with_structured_output(schema)

            messages = []
            if system_prompt:
                messages.append(SystemMessage(content=system_prompt))

            messages.append(HumanMessage(content=prompt))

            result = await structured_llm.ainvoke(messages)
            return cast(T, result)

        except Exception as e:
            logger.error(f"Structured generation failed: {e}")
            full_prompt = f"{system_prompt}\n\n{prompt}" if system_prompt else prompt
            response_text = await self.generate(full_prompt)

            try:
                logger.debug("Falling back to manual JSON parsing")
                return self.parse_json_response(response_text, schema)

            except Exception as parse_error:
                raise RuntimeError(
                    f"Structured validation failed: {parse_error}"
                ) from e
