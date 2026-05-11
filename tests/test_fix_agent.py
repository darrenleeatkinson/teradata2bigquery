from unittest.mock import AsyncMock, MagicMock

import pytest

from td2bq.fix_agent import fix
from td2bq.validator import ValidationResult


@pytest.mark.asyncio
async def test_fix_returns_corrected_sql():
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="SELECT 1")]
    mock_client.messages.create = AsyncMock(return_value=mock_response)

    result = await fix(
        original_sql="SELECT * FROM old_table",
        bq_sql="SELECT * FROM `p.d.t`",
        error=ValidationResult(ok=False, errors=["Syntax error at: FROM"]),
        client=mock_client,
        system_prompt="You are a SQL fixer.",
    )

    assert result == "SELECT 1"
    mock_client.messages.create.assert_called_once()


@pytest.mark.asyncio
async def test_fix_passes_error_to_claude():
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text="FIXED SQL")]
    mock_client.messages.create = AsyncMock(return_value=mock_response)

    await fix(
        original_sql="orig",
        bq_sql="broken",
        error=ValidationResult(ok=False, errors=["Error: column not found"]),
        client=mock_client,
        system_prompt="system",
    )

    call_args = mock_client.messages.create.call_args
    user_message = call_args.kwargs["messages"][0]["content"]
    assert "Error: column not found" in user_message
    assert "broken" in user_message
