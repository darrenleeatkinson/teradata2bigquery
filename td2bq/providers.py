from abc import ABC, abstractmethod
import os


class LLMProvider(ABC):
    """Common interface for LLM backends (Anthropic, Gemini, …)."""

    @abstractmethod
    async def complete(self, system_prompt: str, user_message: str) -> str:
        ...

    @property
    @abstractmethod
    def model_name(self) -> str:
        ...


class AnthropicProvider(LLMProvider):
    DEFAULT_MODEL = "claude-opus-4-5"

    def __init__(self, api_key: str, model: str | None = None):
        import anthropic
        self._client = anthropic.AsyncAnthropic(api_key=api_key)
        self._model = model or self.DEFAULT_MODEL

    @property
    def model_name(self) -> str:
        return self._model

    async def complete(self, system_prompt: str, user_message: str) -> str:
        response = await self._client.messages.create(
            model=self._model,
            max_tokens=8192,
            system=[
                {
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_message}],
        )
        return response.content[0].text.strip()


class GeminiProvider(LLMProvider):
    DEFAULT_MODEL = "gemini-2.0-flash"

    def __init__(self, api_key: str, model: str | None = None):
        try:
            from google import genai  # noqa: PLC0415
        except ImportError:
            raise ImportError(
                "google-genai is required for the Gemini provider. "
                "Install it with: pip install google-genai"
            )
        self._client = genai.Client(api_key=api_key)
        self._model = model or self.DEFAULT_MODEL

    @property
    def model_name(self) -> str:
        return self._model

    async def complete(self, system_prompt: str, user_message: str) -> str:
        from google.genai import types  # noqa: PLC0415
        response = await self._client.aio.models.generate_content(
            model=self._model,
            contents=user_message,
            config=types.GenerateContentConfig(
                system_instruction=system_prompt,
                max_output_tokens=8192,
            ),
        )
        return response.text.strip()


def create_provider(name: str, model: str | None = None) -> LLMProvider:
    """Instantiate a provider by name, reading credentials from environment."""
    if name == "anthropic":
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY is not set")
        return AnthropicProvider(api_key=api_key, model=model)
    if name == "gemini":
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_API_KEY is not set")
        return GeminiProvider(api_key=api_key, model=model)
    raise ValueError(
        f"Unknown provider '{name}'. Supported providers: anthropic, gemini"
    )
