from .providers import LLMProvider
from .validator import ValidationResult


async def fix(
    original_sql: str,
    bq_sql: str,
    error: ValidationResult,
    provider: LLMProvider,
    system_prompt: str,
) -> str:
    error_text = "\n".join(error.errors)
    user_message = (
        f"The following BigQuery SQL failed validation:\n\n"
        f"ERROR:\n{error_text}\n\n"
        f"FAILING SQL:\n{bq_sql}\n\n"
        f"Fix the SQL so it is valid BigQuery Standard SQL. "
        f"Return ONLY the fixed SQL — no explanation, no markdown fences."
    )
    return await provider.complete(system_prompt, user_message)
