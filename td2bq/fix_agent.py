import anthropic

from .validator import ValidationResult


async def fix(
    original_sql: str,
    bq_sql: str,
    error: ValidationResult,
    client: anthropic.AsyncAnthropic,
    system_prompt: str,
) -> str:
    error_text = "\n".join(error.errors)
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
                    f"The following BigQuery SQL failed validation:\n\n"
                    f"ERROR:\n{error_text}\n\n"
                    f"FAILING SQL:\n{bq_sql}\n\n"
                    f"Fix the SQL so it is valid BigQuery Standard SQL. "
                    f"Return ONLY the fixed SQL — no explanation, no markdown fences."
                ),
            }
        ],
    )
    return response.content[0].text.strip()
