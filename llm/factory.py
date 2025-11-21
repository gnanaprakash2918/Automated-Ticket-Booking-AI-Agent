import os
from typing import Optional
from loguru import logger
from .interface import LLMInterface
from .gemini import GeminiLLM
from .ollama import OllamaLLM

class LLMFactory:
    @staticmethod
    def create_llm(
        provider: str = "gemini",
        prompt_dir: str = "services/tnstc/prompts",
        **kwargs
    ) -> LLMInterface:
        """
        Factory method to create an LLM instance based on the provider.
        
        Args:
            provider: "gemini" or "ollama"
            prompt_dir: Directory containing prompt templates
            **kwargs: Additional arguments passed to the LLM constructor
        """
        provider = provider.lower()
        logger.info(f"LLMFactory: Creating LLM for provider '{provider}'")
        
        if provider == "gemini":
            return GeminiLLM(prompt_dir=prompt_dir, **kwargs)
        elif provider == "ollama":
            return OllamaLLM(prompt_dir=prompt_dir, **kwargs)
        else:
            raise ValueError(f"Unknown LLM provider: {provider}")

    @staticmethod
    def get_default_llm(prompt_dir: str = "services/tnstc/prompts") -> LLMInterface:
        """
        Returns the default LLM based on environment variables or hardcoded preference.
        """
        provider = os.getenv("LLM_PROVIDER", "gemini")
        return LLMFactory.create_llm(provider, prompt_dir)
