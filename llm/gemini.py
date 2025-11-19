import os
from typing import Any, Optional, Type, cast
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_google_genai import ChatGoogleGenerativeAI
from llm.interface import LLMInterface, T


class GeminiLLM(LLMInterface):
    """
    Adapter for Gemini
    """

    def __init__(
        self, 
        model_name: Optional[str] = None, 
        api_key_env: str = "GEMINI_API_KEY", 
        prompt_dir: str = "prompts",
        timeout: int = 60
    ):
        super().__init__(prompt_dir=prompt_dir)

        model = model_name or os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
        api_key = os.getenv(api_key_env) or os.getenv("GOOGLE_API_KEY")

        if not api_key:
            raise ValueError("Gemini API key not set.")
        
        self.llm = ChatGoogleGenerativeAI(
            model=model, 
            api_key=api_key, 
            request_timeout=timeout, 
            temperature=0.0
        )

    async def generate(self, prompt: str, **kwargs: Any) -> str:
        try:
            response = await self.llm.ainvoke(prompt)
            content = getattr(response, "content", str(response))
            return str(content)
        
        except Exception as e:
            raise RuntimeError(f"Gemini generation failed: {e}") from e

    async def generate_structured(
        self, 
        schema: Type[T], 
        prompt: str, 
        system_prompt: str = "", 
        **kwargs: Any
    ) -> T:
        try:
            structured_llm = self.llm.with_structured_output(schema)

            messages = []
            if system_prompt:
                messages.append(SystemMessage(content=system_prompt))
            
            messages.append(HumanMessage(content=prompt))

            result = await structured_llm.ainvoke(messages)
            return cast(T, result)
        
        except Exception as e:
            full_prompt = f"{system_prompt}\n\n{prompt}" if system_prompt else prompt
            response_text = await self.generate(full_prompt)

            try:
                clean_json = response_text.strip().strip("`").replace("json\n", "")
                return cast(T, schema.model_validate_json(clean_json))
            
            except Exception as parse_error:
                raise RuntimeError(f"Structured validation failed: {parse_error}") from e