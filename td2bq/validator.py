from dataclasses import dataclass, field

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery


@dataclass
class ValidationResult:
    ok: bool
    errors: list[str] = field(default_factory=list)
    rows_affected: int | None = None


def validate(
    sql: str,
    client: bigquery.Client,
    execute: bool = False,
) -> ValidationResult:
    try:
        if execute:
            job = client.query(sql)
            job.result()
            return ValidationResult(ok=True, rows_affected=job.num_dml_affected_rows)
        else:
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            client.query(sql, job_config=job_config)
            return ValidationResult(ok=True)
    except GoogleAPIError as e:
        return ValidationResult(ok=False, errors=[str(e)])
    except Exception as e:
        return ValidationResult(ok=False, errors=[f"Unexpected error: {e}"])
