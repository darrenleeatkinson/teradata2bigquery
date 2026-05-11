from pathlib import Path

import anthropic

from .classifier import ScriptType

_SYSTEM_PROMPT: str | None = None
_SYSTEM_PROMPT_PATH = Path(__file__).parent / "prompts" / "system.md"


def load_system_prompt() -> str:
    global _SYSTEM_PROMPT
    if _SYSTEM_PROMPT is None:
        _SYSTEM_PROMPT = _SYSTEM_PROMPT_PATH.read_text(encoding="utf-8")
    return _SYSTEM_PROMPT


async def translate(
    sql: str,
    script_type: ScriptType,
    client: anthropic.AsyncAnthropic,
    project_id: str,
) -> str:
    system_prompt = load_system_prompt()
    response = await client.messages.create(
        model="claude-opus-4-5",
        max_tokens=8192,
        system=[
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[
            {
                "role": "user",
                "content": (
                    f"Convert the following Teradata BTEQ script to BigQuery Standard SQL.\n"
                    f"Script type: {script_type.value}\n"
                    f"GCP project ID: {project_id}\n\n"
                    f"Return ONLY the BigQuery SQL — no explanation, no markdown fences.\n\n"
                    f"{sql}"
                ),
            }
        ],
    )
    return response.content[0].text.strip()
