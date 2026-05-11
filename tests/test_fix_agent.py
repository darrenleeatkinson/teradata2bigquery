from unittest.mock import AsyncMock, MagicMock

import pytest

from td2bq.fix_agent import fix
from td2bq.providers import LLMProvider
from td2bq.validator import ValidationResult


def _mock_provider(return_text: str) -> LLMProvider:
    provider = MagicMock(spec=LLMProvider)
    provider.complete = AsyncMock(return_value=return_text)
    return provider


@pytest.mark.asyncio
async def test_fix_returns_corrected_sql():
    provider = _mock_provider("SELECT 1")
    result = await fix(
        original_sql="SELECT * FROM old_table",
        bq_sql="SELECT * FROM `p.d.t`",
        error=ValidationResult(ok=False, errors=["Syntax error at: FROM"]),
        provider=provider,
        system_prompt="You are a SQL fixer.",
    )
    assert result == "SELECT 1"
    provider.complete.assert_called_once()


@pytest.mark.asyncio
async def test_fix_passes_error_to_provider():
    provider = _mock_provider("FIXED SQL")
    await fix(
        original_sql="orig",
        bq_sql="broken",
        error=ValidationResult(ok=False, errors=["Error: column not found"]),
        provider=provider,
        system_prompt="system",
    )
    _system, user_message = provider.complete.call_args.args
    assert "Error: column not found" in user_message
    assert "broken" in user_message
