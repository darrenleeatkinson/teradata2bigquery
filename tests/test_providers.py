from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from td2bq.providers import AnthropicProvider, create_provider


@pytest.mark.asyncio
async def test_anthropic_provider_returns_stripped_text():
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="  SELECT 1  ")]

    with patch("anthropic.AsyncAnthropic") as MockClient:
        instance = MockClient.return_value
        instance.messages.create = AsyncMock(return_value=mock_response)
        provider = AnthropicProvider(api_key="test-key")
        result = await provider.complete("system", "user")

    assert result == "SELECT 1"


@pytest.mark.asyncio
async def test_anthropic_provider_sends_cache_control():
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="ok")]

    with patch("anthropic.AsyncAnthropic") as MockClient:
        instance = MockClient.return_value
        instance.messages.create = AsyncMock(return_value=mock_response)
        provider = AnthropicProvider(api_key="test-key")
        await provider.complete("sys", "usr")

        call_kwargs = instance.messages.create.call_args.kwargs
        system_block = call_kwargs["system"][0]
        assert system_block["cache_control"] == {"type": "ephemeral"}


def test_anthropic_default_model():
    provider = AnthropicProvider(api_key="x")
    assert provider.model_name == AnthropicProvider.DEFAULT_MODEL


def test_anthropic_custom_model():
    provider = AnthropicProvider(api_key="x", model="claude-haiku-4-5-20251001")
    assert provider.model_name == "claude-haiku-4-5-20251001"


def test_create_provider_anthropic(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    provider = create_provider("anthropic")
    assert isinstance(provider, AnthropicProvider)
    assert provider.model_name == AnthropicProvider.DEFAULT_MODEL


def test_create_provider_anthropic_custom_model(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    provider = create_provider("anthropic", model="claude-haiku-4-5-20251001")
    assert provider.model_name == "claude-haiku-4-5-20251001"


def test_create_provider_anthropic_missing_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
        create_provider("anthropic")


def test_create_provider_gemini_missing_key(monkeypatch):
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    with pytest.raises(ValueError, match="GOOGLE_API_KEY"):
        create_provider("gemini")


def test_create_provider_unknown_raises():
    with pytest.raises(ValueError, match="Unknown provider"):
        create_provider("openai")
