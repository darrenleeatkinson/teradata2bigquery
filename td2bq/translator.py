from pathlib import Path

from .classifier import ScriptType
from .providers import LLMProvider

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
    provider: LLMProvider,
    project_id: str,
) -> str:
    system_prompt = load_system_prompt()
    user_message = (
        f"Convert the following Teradata BTEQ script to BigQuery Standard SQL.\n"
        f"Script type: {script_type.value}\n"
        f"GCP project ID: {project_id}\n\n"
        f"Return ONLY the BigQuery SQL — no explanation, no markdown fences.\n\n"
        f"{sql}"
    )
    return await provider.complete(system_prompt, user_message)
