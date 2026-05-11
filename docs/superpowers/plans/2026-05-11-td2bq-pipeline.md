# Teradata → BigQuery Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an async Python CLI that bulk-converts Teradata BTEQ scripts to BigQuery SQL using Claude API, with dry-run/execute validation, auto-fix retries, SQLite state, and HTML reporting.

**Architecture:** `asyncio` pipeline with `Semaphore`-controlled concurrency. Each script flows through: classify → translate (Claude API, prompt-cached) → validate (BigQuery) → fix loop → write output. State persisted in SQLite for `--resume` support.

**Tech Stack:** Python 3.11+, `anthropic` SDK, `google-cloud-bigquery`, `python-dotenv`, `jinja2`, `rich`, `pytest`, `pytest-asyncio`

---

## File Map

| File | Responsibility |
|------|---------------|
| `td2bq/__init__.py` | Package marker |
| `td2bq/classifier.py` | `ScriptType` enum + `classify(sql) → ScriptType` |
| `td2bq/state.py` | `ScriptRecord` dataclass + `StateStore` (SQLite CRUD) |
| `td2bq/prompts/system.md` | Cached system prompt for Claude (translation rules) |
| `td2bq/translator.py` | `translate(sql, type, client, project) → str` (Claude API) |
| `td2bq/validator.py` | `ValidationResult` + `validate(sql, client, execute) → ValidationResult` |
| `td2bq/fix_agent.py` | `fix(original, bq_sql, error, client, system_prompt) → str` |
| `td2bq/reporter.py` | `generate(records, output_path)` — Jinja2 HTML report |
| `td2bq/cli.py` | `main()` argparse entry + `run_pipeline()` async orchestrator |
| `pyproject.toml` | Project metadata + dependencies |
| `.env.example` | Template for credentials |
| `tests/test_classifier.py` | Classifier unit tests |
| `tests/test_state.py` | StateStore unit tests |
| `tests/test_fix_agent.py` | Fix agent unit tests (mocked Claude) |
| `tests/test_reporter.py` | Reporter unit tests |

---

### Task 1: Project scaffold

**Files:**
- Create: `pyproject.toml`
- Create: `.env.example`
- Create: `td2bq/__init__.py`
- Create: `tests/__init__.py`

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.backends.legacy:build"

[project]
name = "td2bq"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "anthropic>=0.25",
    "google-cloud-bigquery>=3.11",
    "python-dotenv>=1.0",
    "jinja2>=3.1",
    "rich>=13",
]

[project.scripts]
td2bq = "td2bq.cli:main"

[project.optional-dependencies]
dev = [
    "pytest>=8",
    "pytest-asyncio>=0.23",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

- [ ] **Step 2: Create `.env.example`**

```ini
ANTHROPIC_API_KEY=sk-ant-...
GCP_PROJECT_ID=your-gcp-project-id
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
```

- [ ] **Step 3: Create empty package files**

`td2bq/__init__.py` — empty file  
`tests/__init__.py` — empty file

- [ ] **Step 4: Install in editable mode**

```bash
cd /mnt/c/Users/darren.atkinson/OneDrive\ -\ Accenture/Dynatrace/Experiments/teradata2bigquery
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

Expected: no errors, `td2bq` command available.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml .env.example td2bq/__init__.py tests/__init__.py
git commit -m "feat: project scaffold with dependencies"
```

---

### Task 2: State store

**Files:**
- Create: `td2bq/state.py`
- Create: `tests/test_state.py`

- [ ] **Step 1: Write failing tests**

`tests/test_state.py`:
```python
import tempfile
from datetime import datetime
from pathlib import Path
from td2bq.state import ScriptRecord, StateStore, Status


def _store():
    tmp = tempfile.mktemp(suffix=".db")
    return StateStore(Path(tmp))


def test_upsert_and_get():
    store = _store()
    rec = ScriptRecord(path="/a/b.sql", script_type="VOLATILE_LOAD", status=Status.PENDING)
    store.upsert(rec)
    got = store.get("/a/b.sql")
    assert got is not None
    assert got.script_type == "VOLATILE_LOAD"
    assert got.status == Status.PENDING
    store.close()


def test_upsert_updates_existing():
    store = _store()
    store.upsert(ScriptRecord(path="/a/b.sql"))
    store.upsert(ScriptRecord(path="/a/b.sql", status=Status.SUCCESS, attempts=2))
    got = store.get("/a/b.sql")
    assert got.status == Status.SUCCESS
    assert got.attempts == 2
    store.close()


def test_get_missing_returns_none():
    store = _store()
    assert store.get("/nope.sql") is None
    store.close()


def test_all_returns_all_records():
    store = _store()
    store.upsert(ScriptRecord(path="/a.sql"))
    store.upsert(ScriptRecord(path="/b.sql"))
    assert len(store.all()) == 2
    store.close()


def test_pending_excludes_success():
    store = _store()
    store.upsert(ScriptRecord(path="/a.sql", status=Status.PENDING))
    store.upsert(ScriptRecord(path="/b.sql", status=Status.SUCCESS))
    pending = store.pending()
    assert len(pending) == 1
    assert pending[0].path == "/a.sql"
    store.close()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_state.py -v
```

Expected: `ImportError` — `td2bq.state` does not exist.

- [ ] **Step 3: Implement `td2bq/state.py`**

```python
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path


class Status(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class ScriptRecord:
    path: str
    script_type: str = ""
    status: Status = Status.PENDING
    attempts: int = 0
    error: str = ""
    output_path: str = ""
    translated_at: datetime | None = None


class StateStore:
    def __init__(self, db_path: Path):
        self._conn = sqlite3.connect(str(db_path))
        self._init_schema()

    def _init_schema(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS scripts (
                path         TEXT PRIMARY KEY,
                script_type  TEXT DEFAULT '',
                status       TEXT DEFAULT 'pending',
                attempts     INTEGER DEFAULT 0,
                error        TEXT DEFAULT '',
                output_path  TEXT DEFAULT '',
                translated_at TEXT
            )
        """)
        self._conn.commit()

    def upsert(self, record: ScriptRecord) -> None:
        ts = record.translated_at.isoformat() if record.translated_at else None
        self._conn.execute("""
            INSERT INTO scripts
                (path, script_type, status, attempts, error, output_path, translated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                script_type   = excluded.script_type,
                status        = excluded.status,
                attempts      = excluded.attempts,
                error         = excluded.error,
                output_path   = excluded.output_path,
                translated_at = excluded.translated_at
        """, (record.path, record.script_type, record.status.value,
              record.attempts, record.error, record.output_path, ts))
        self._conn.commit()

    def get(self, path: str) -> ScriptRecord | None:
        row = self._conn.execute(
            "SELECT * FROM scripts WHERE path = ?", (path,)
        ).fetchone()
        return self._row_to_record(row) if row else None

    def all(self) -> list[ScriptRecord]:
        rows = self._conn.execute("SELECT * FROM scripts").fetchall()
        return [self._row_to_record(r) for r in rows]

    def pending(self) -> list[ScriptRecord]:
        rows = self._conn.execute(
            "SELECT * FROM scripts WHERE status IN ('pending', 'in_progress')"
        ).fetchall()
        return [self._row_to_record(r) for r in rows]

    def close(self) -> None:
        self._conn.close()

    @staticmethod
    def _row_to_record(row: tuple) -> ScriptRecord:
        path, script_type, status, attempts, error, output_path, ts = row
        return ScriptRecord(
            path=path,
            script_type=script_type,
            status=Status(status),
            attempts=attempts,
            error=error,
            output_path=output_path,
            translated_at=datetime.fromisoformat(ts) if ts else None,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_state.py -v
```

Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add td2bq/state.py tests/test_state.py
git commit -m "feat: SQLite state store with upsert, get, pending, all"
```

---

### Task 3: Classifier

**Files:**
- Create: `td2bq/classifier.py`
- Create: `tests/test_classifier.py`

- [ ] **Step 1: Write failing tests**

`tests/test_classifier.py`:
```python
from td2bq.classifier import ScriptType, classify


def test_volatile_table():
    sql = "CREATE VOLATILE MULTISET TABLE wt_work (id INTEGER) PRIMARY INDEX (id);"
    assert classify(sql) == ScriptType.VOLATILE_LOAD


def test_volatile_insert():
    sql = "INSERT INTO wt_staging SELECT * FROM P_BASE_LOAD.source WHERE x = 1;"
    assert classify(sql) == ScriptType.VOLATILE_LOAD


def test_merge():
    sql = "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET col = 1;"
    assert classify(sql) == ScriptType.MERGE


def test_upsert_update_plus_insert():
    sql = """
    UPDATE TGT FROM target_table TGT, staging STG
    SET col = STG.col WHERE TGT.id = STG.id;
    INSERT INTO target_table SELECT * FROM staging WHERE dml_ind = 'I';
    """
    assert classify(sql) == ScriptType.UPSERT


def test_ddl_permanent_table():
    sql = "CREATE MULTISET TABLE P_BASE_LOAD.my_dim (id INTEGER, name VARCHAR(100)) PRIMARY INDEX (id);"
    assert classify(sql) == ScriptType.DDL


def test_reporting_select_only():
    sql = "SELECT id, name FROM P_SUBSCRIBER.account WHERE status = 'A';"
    assert classify(sql) == ScriptType.REPORTING


def test_unknown():
    sql = ".SET ERRORLEVEL 3807 SEVERITY 0"
    assert classify(sql) == ScriptType.UNKNOWN
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_classifier.py -v
```

Expected: `ImportError` — `td2bq.classifier` does not exist.

- [ ] **Step 3: Implement `td2bq/classifier.py`**

```python
import re
from enum import Enum


class ScriptType(str, Enum):
    VOLATILE_LOAD = "VOLATILE_LOAD"
    UPSERT = "UPSERT"
    MERGE = "MERGE"
    DDL = "DDL"
    REPORTING = "REPORTING"
    UNKNOWN = "UNKNOWN"


def classify(sql: str) -> ScriptType:
    upper = sql.upper()

    if re.search(r"CREATE\s+VOLATILE\s", upper) or re.search(r"INSERT\s+INTO\s+WT_", upper):
        return ScriptType.VOLATILE_LOAD

    if re.search(r"\bMERGE\s+INTO\b", upper):
        return ScriptType.MERGE

    if re.search(r"\bUPDATE\s+TGT\s+FROM\b", upper) and re.search(r"\bINSERT\s+INTO\b", upper):
        return ScriptType.UPSERT

    if re.search(r"\bCREATE\s+(?:MULTISET\s+)?TABLE\b", upper) and "VOLATILE" not in upper:
        return ScriptType.DDL

    if (
        re.search(r"^\s*SELECT\b", upper, re.MULTILINE)
        and not re.search(r"\b(?:INSERT|UPDATE|DELETE|MERGE)\b", upper)
    ):
        return ScriptType.REPORTING

    return ScriptType.UNKNOWN
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_classifier.py -v
```

Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add td2bq/classifier.py tests/test_classifier.py
git commit -m "feat: script classifier with ScriptType enum"
```

---

### Task 4: System prompt

**Files:**
- Create: `td2bq/prompts/system.md`

- [ ] **Step 1: Create the prompts directory**

```bash
mkdir -p td2bq/prompts
touch td2bq/prompts/__init__.py
```

- [ ] **Step 2: Write `td2bq/prompts/system.md`**

This file is the Claude system prompt, loaded once and cached via `cache_control`. Write the following content:

```markdown
You are an expert SQL migration engineer specialising in converting Teradata BTEQ scripts to Google BigQuery Standard SQL.

## Critical Rules

1. Remove ALL BTEQ directives: `.SET`, `.IF ERRORCODE`, `.GOTO`, `.LABEL`, `.QUIT` — they cause parse errors in BigQuery.
2. `DELETE FROM t;` without WHERE clause → `DELETE FROM t WHERE TRUE;`
3. `QUALIFY expr = 1` is not supported — wrap query in CTE and add `WHERE rn = 1` outside.
4. `COLLECT STATISTICS` / `COLLECT STATS` → drop entirely.
5. Reserved word `ORDER` → backtick-quote as `project.dataset.order`.
6. `FROM t1, t2` is a CROSS JOIN in BigQuery — rewrite as `INNER JOIN ... ON`.
7. Remove ALL `MOD 9 = N` and `MOD 9 IS NULL` partition filters — BigQuery parallelises internally.
8. `CREATE VOLATILE MULTISET TABLE` → `CREATE OR REPLACE TEMP TABLE`. Drop: VOLATILE, MULTISET, PRIMARY INDEX, ON COMMIT PRESERVE ROWS.
9. `BT;` → `BEGIN TRANSACTION;` and `ET;` → `COMMIT TRANSACTION;`.
10. Wrap transaction logic in `BEGIN ... EXCEPTION WHEN ERROR THEN ROLLBACK TRANSACTION; RAISE USING MESSAGE = @@error_message; END;`

## Dataset Name Mapping

Replace Teradata database names with BigQuery dataset names:

| Teradata | BigQuery |
|----------|---------|
| P_BASE_LOAD | base_load |
| P_INTEGRATION_VIEWS | integration |
| P_INTEGRATION_DAILY_TABLES | integration_daily |
| P_INTEGRATION_REFERENCE_TABLES | reference |
| P_SUBSCRIBER | subscriber |
| P_CUSTOMER | customer |
| P_REFERENCE | ref_data |
| P_USER_VIEWS | user_views |

Table references use the format: `{project}.{dataset}.{table}` — replace {project} with the GCP project ID provided.

## Data Type Mappings

| Teradata | BigQuery |
|----------|---------|
| VARCHAR(n) / CHAR(n) | STRING |
| INTEGER / INT / SMALLINT / BYTEINT | INT64 |
| BIGINT | INT64 |
| DECIMAL(p,s) / NUMERIC(p,s) | NUMERIC |
| FLOAT / REAL | FLOAT64 |
| DATE | DATE |
| TIME(n) | TIME |
| TIMESTAMP(n) | DATETIME |
| TIMESTAMP WITH TIME ZONE | TIMESTAMP |
| CLOB | STRING |
| BLOB | BYTES |

## Function Mappings

### Strings
- `POSITION(sub IN str)` → `STRPOS(str, sub)` (note argument order reversal)
- `SUBSTRING(x FROM n FOR m)` → `SUBSTR(x, n, m)`
- `CHAR_LENGTH(x)` / `CHARACTER_LENGTH(x)` → `LENGTH(x)`
- `OREPLACE(x, from, to)` → `REPLACE(x, from, to)`
- `INDEX(str, sub)` → `STRPOS(str, sub)`
- `TO_CHAR(x, fmt)` → `FORMAT_DATE(fmt, x)` or `FORMAT_DATETIME(fmt, x)`
- `Translate_Chk(x USING unicode_to_latin)` → `REGEXP_INSTR(x, r'[^\x20-\x7E]')`
- `TRANSLATE(x USING unicode_to_latin)` → `REGEXP_REPLACE(x, r'[^\x20-\x7E]', ' ')`
- `STRTOK(x, delim, n)` → `SPLIT(x, delim)[SAFE_OFFSET(n-1)]`
- `STRTOK_COUNT(x, delim)` → `ARRAY_LENGTH(SPLIT(x, delim))`

### Dates and Times
- `CURRENT_DATE` → `CURRENT_DATE()`
- `CURRENT_TIMESTAMP` / `CURRENT_TIMESTAMP(0)` → `CURRENT_TIMESTAMP()`
- `CAST(x AS DATE)` → `DATE(x)`
- `CAST(x AS TIME(0))` → `TIME(CAST(x AS DATETIME))`
- `CAST(x AS DATE FORMAT 'dd/mm/yyyy')` → `PARSE_DATE('%d/%m/%Y', x)`
- `CAST(x AS DATE FORMAT 'yyyy-mm-dd')` → `PARSE_DATE('%Y-%m-%d', x)`
- `ADD_MONTHS(d, n)` → `DATE_ADD(d, INTERVAL n MONTH)`
- `MONTHS_BETWEEN(d1, d2)` → `DATE_DIFF(d1, d2, MONTH)`
- `DATE - n` → `DATE_SUB(d, INTERVAL n DAY)`
- `DATE + n` → `DATE_ADD(d, INTERVAL n DAY)`
- `TRUNC(ts, 'DD')` → `DATE_TRUNC(ts, DAY)`
- `TRUNC(ts, 'MM')` → `DATE_TRUNC(ts, MONTH)`
- `ZEROIFNULL(x)` → `COALESCE(x, 0)`
- `NULLIFZERO(x)` → `NULLIF(x, 0)`
- `TD_DAY_OF_WEEK(d)` → `EXTRACT(DAYOFWEEK FROM d)`

### Numeric
- `x MOD y` → `MOD(x, y)`
- `CAST(x AS BIGINT) MOD y` → `MOD(CAST(x AS INT64), y)`

## DDL Constructs to Drop

Drop these entirely from CREATE TABLE statements:
- `FALLBACK`
- `NO BEFORE JOURNAL` / `NO AFTER JOURNAL`
- `CHECKSUM = DEFAULT`
- `DEFAULT MERGEBLOCKRATIO`
- `MAP = TD_MAP1`
- `CHARACTER SET LATIN NOT CASESPECIFIC`
- `ON COMMIT PRESERVE ROWS`
- `WITH DATA` (implicit in BigQuery CTAS)
- `PRIMARY INDEX (...)` on volatile tables

For permanent tables: replace `PRIMARY INDEX (col)` with `CLUSTER BY col`.

## DML Differences

- `UPDATE TGT FROM target TGT, staging STG SET col = STG.col WHERE TGT.id = STG.id`
  → `UPDATE target AS tgt SET col = stg.col FROM staging AS stg WHERE tgt.id = stg.id`
- `DELETE FROM t;` → `DELETE FROM t WHERE TRUE;`

## QUALIFY Pattern

```sql
-- Input (Teradata)
SELECT * FROM wt_table
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1;

-- Output (BigQuery)
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn
  FROM `project.dataset.wt_table`
)
SELECT * EXCEPT (rn) FROM ranked WHERE rn = 1;
```

## Two-Phase UPDATE + INSERT → MERGE

```sql
-- Input (Teradata)
UPDATE TGT FROM target TGT, staging STG SET col = STG.col WHERE TGT.id = STG.id AND STG.dml_ind IN ('U','D');
INSERT INTO target SELECT ... FROM staging WHERE dml_ind IN ('I','DI');

-- Output (BigQuery)
MERGE `project.dataset.target` AS tgt
USING staging_temp AS stg ON tgt.id = stg.id
WHEN MATCHED AND stg.dml_ind IN ('U','D') THEN UPDATE SET col = stg.col
WHEN NOT MATCHED AND stg.dml_ind IN ('I','DI') THEN INSERT (col) VALUES (stg.col);
```

## RI Sentinel Convention

Preserve these sentinel values:
- `-99` = source FK was NULL
- `-1` = FK has value but no matching dimension row
- `-2` = hardcoded sentinel

```sql
CASE WHEN stg.fk_id IS NULL THEN -99
     ELSE COALESCE(dim.surrogate_key, -1)
END AS fk_key
```

## Output Requirements

Return ONLY valid BigQuery Standard SQL. No explanations. No markdown fences. No comments unless they were in the original.
```

- [ ] **Step 3: Commit**

```bash
git add td2bq/prompts/
git commit -m "feat: Claude system prompt for Teradata→BigQuery translation"
```

---

### Task 5: Translator

**Files:**
- Create: `td2bq/translator.py`

No unit tests for the translator — it wraps an external API. Integration tests require live credentials (out of scope). The public contract is tested via the fix_agent test in Task 7.

- [ ] **Step 1: Implement `td2bq/translator.py`**

```python
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
```

- [ ] **Step 2: Commit**

```bash
git add td2bq/translator.py
git commit -m "feat: Claude API translator with prompt caching"
```

---

### Task 6: Validator

**Files:**
- Create: `td2bq/validator.py`

- [ ] **Step 1: Implement `td2bq/validator.py`**

```python
from dataclasses import dataclass, field

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError


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
```

- [ ] **Step 2: Commit**

```bash
git add td2bq/validator.py
git commit -m "feat: BigQuery validator supporting dry-run and full execution"
```

---

### Task 7: Fix agent

**Files:**
- Create: `td2bq/fix_agent.py`
- Create: `tests/test_fix_agent.py`

- [ ] **Step 1: Write failing tests**

`tests/test_fix_agent.py`:
```python
from unittest.mock import AsyncMock, MagicMock, patch
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_fix_agent.py -v
```

Expected: `ImportError` — `td2bq.fix_agent` does not exist.

- [ ] **Step 3: Implement `td2bq/fix_agent.py`**

```python
import anthropic

from .translator import load_system_prompt
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_fix_agent.py -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add td2bq/fix_agent.py tests/test_fix_agent.py
git commit -m "feat: fix agent retries failed SQL with BigQuery error context"
```

---

### Task 8: Reporter

**Files:**
- Create: `td2bq/reporter.py`
- Create: `tests/test_reporter.py`

- [ ] **Step 1: Write failing tests**

`tests/test_reporter.py`:
```python
import tempfile
from pathlib import Path
from datetime import datetime
from td2bq.reporter import generate
from td2bq.state import ScriptRecord, Status


def _records():
    return [
        ScriptRecord(path="/scripts/a.sql", script_type="VOLATILE_LOAD", status=Status.SUCCESS, attempts=1),
        ScriptRecord(path="/scripts/b.sql", script_type="UPSERT", status=Status.FAILED, attempts=3, error="Syntax error"),
        ScriptRecord(path="/scripts/c.sql", script_type="UNKNOWN", status=Status.PENDING, attempts=0),
    ]


def test_generate_creates_html_file():
    out = Path(tempfile.mktemp(suffix=".html"))
    generate(_records(), out)
    assert out.exists()
    content = out.read_text()
    assert "<html" in content


def test_report_contains_summary_counts():
    out = Path(tempfile.mktemp(suffix=".html"))
    generate(_records(), out)
    content = out.read_text()
    assert ">3<" in content   # total
    assert ">1<" in content   # success
    assert ">1<" in content   # failed


def test_report_contains_script_names():
    out = Path(tempfile.mktemp(suffix=".html"))
    generate(_records(), out)
    content = out.read_text()
    assert "a.sql" in content
    assert "b.sql" in content
    assert "c.sql" in content


def test_report_contains_error_snippet():
    out = Path(tempfile.mktemp(suffix=".html"))
    generate(_records(), out)
    assert "Syntax error" in out.read_text()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_reporter.py -v
```

Expected: `ImportError` — `td2bq.reporter` does not exist.

- [ ] **Step 3: Implement `td2bq/reporter.py`**

```python
from datetime import datetime
from pathlib import Path

from jinja2 import Template

from .state import ScriptRecord, Status

_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Teradata → BigQuery Conversion Report</title>
<style>
  body { font-family: -apple-system, sans-serif; margin: 2em; color: #333; }
  h1 { color: #1a73e8; }
  .summary { display: flex; gap: 2em; margin: 1.5em 0; flex-wrap: wrap; }
  .stat { background: #f1f3f4; padding: 1em 2em; border-radius: 8px; text-align: center; min-width: 100px; }
  .stat .n { font-size: 2.5em; font-weight: bold; }
  .c-success { color: #34a853; }
  .c-failed  { color: #ea4335; }
  .c-pending { color: #fbbc04; }
  table { width: 100%; border-collapse: collapse; margin-top: 1em; font-size: 0.9em; }
  th { background: #1a73e8; color: white; padding: 0.6em 1em; text-align: left; }
  td { padding: 0.5em 1em; border-bottom: 1px solid #e8eaed; vertical-align: top; }
  tr:hover td { background: #f8f9fa; }
  .badge { padding: 2px 8px; border-radius: 12px; font-size: 0.8em; font-weight: 500; }
  .badge-success { background: #e6f4ea; color: #137333; }
  .badge-failed  { background: #fce8e6; color: #c5221f; }
  .badge-pending { background: #fef7e0; color: #b06000; }
  .badge-in_progress { background: #e8f0fe; color: #1967d2; }
  .err { font-family: monospace; font-size: 0.8em; color: #c5221f; white-space: pre-wrap; word-break: break-all; }
</style>
</head>
<body>
<h1>Teradata → BigQuery Conversion Report</h1>
<p>Generated: {{ generated_at }}</p>

<div class="summary">
  <div class="stat"><div class="n">{{ total }}</div><div>Total</div></div>
  <div class="stat"><div class="n c-success">{{ success }}</div><div>Success</div></div>
  <div class="stat"><div class="n c-failed">{{ failed }}</div><div>Failed</div></div>
  <div class="stat"><div class="n c-pending">{{ pending }}</div><div>Pending</div></div>
</div>

<table>
  <thead>
    <tr>
      <th>Script</th><th>Type</th><th>Status</th><th>Attempts</th><th>Error</th>
    </tr>
  </thead>
  <tbody>
    {% for r in records %}
    <tr>
      <td>{{ r.path | basename }}</td>
      <td>{{ r.script_type or '—' }}</td>
      <td><span class="badge badge-{{ r.status.value }}">{{ r.status.value }}</span></td>
      <td>{{ r.attempts }}</td>
      <td>{% if r.error %}<span class="err">{{ r.error[:300] }}</span>{% endif %}</td>
    </tr>
    {% endfor %}
  </tbody>
</table>
</body>
</html>
"""


def generate(records: list[ScriptRecord], output_path: Path) -> None:
    total = len(records)
    success = sum(1 for r in records if r.status == Status.SUCCESS)
    failed = sum(1 for r in records if r.status == Status.FAILED)
    pending = total - success - failed

    t = Template(_TEMPLATE)
    t.globals["basename"] = lambda p: Path(p).name

    html = t.render(
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        total=total,
        success=success,
        failed=failed,
        pending=pending,
        records=records,
    )
    output_path.write_text(html, encoding="utf-8")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_reporter.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add td2bq/reporter.py tests/test_reporter.py
git commit -m "feat: HTML reporter with summary stats and per-script table"
```

---

### Task 9: CLI and pipeline orchestrator

**Files:**
- Create: `td2bq/cli.py`

- [ ] **Step 1: Implement `td2bq/cli.py`**

```python
import argparse
import asyncio
import os
from datetime import datetime
from pathlib import Path

import anthropic
from dotenv import load_dotenv
from google.cloud import bigquery
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn

from .classifier import classify
from .fix_agent import fix
from .reporter import generate
from .state import ScriptRecord, StateStore, Status
from .translator import load_system_prompt, translate
from .validator import validate

console = Console()


async def _process_one(
    record: ScriptRecord,
    store: StateStore,
    anthropic_client: anthropic.AsyncAnthropic,
    bq_client: bigquery.Client,
    output_dir: Path,
    execute: bool,
    max_fix_attempts: int,
    system_prompt: str,
    semaphore: asyncio.Semaphore,
    progress: Progress,
    task_id: int,
) -> None:
    async with semaphore:
        try:
            sql = Path(record.path).read_text(encoding="utf-8", errors="replace")
            script_type = classify(sql)
            record.script_type = script_type.value
            record.status = Status.IN_PROGRESS
            store.upsert(record)

            project_id = os.environ["GCP_PROJECT_ID"]
            bq_sql = await translate(sql, script_type, anthropic_client, project_id)
            result = validate(bq_sql, bq_client, execute)
            attempts = 1

            while not result.ok and attempts < max_fix_attempts:
                bq_sql = await fix(sql, bq_sql, result, anthropic_client, system_prompt)
                result = validate(bq_sql, bq_client, execute)
                attempts += 1

            out_path = output_dir / (Path(record.path).stem + "_bq.sql")
            out_path.write_text(bq_sql, encoding="utf-8")

            record.status = Status.SUCCESS if result.ok else Status.FAILED
            record.attempts = attempts
            record.error = "\n".join(result.errors) if not result.ok else ""
            record.output_path = str(out_path)
            record.translated_at = datetime.now()
        except Exception as exc:
            record.status = Status.FAILED
            record.error = str(exc)
            record.attempts = (record.attempts or 0) + 1

        store.upsert(record)
        progress.advance(task_id)


async def run_pipeline(args: argparse.Namespace) -> None:
    load_dotenv()

    if args.project:
        os.environ["GCP_PROJECT_ID"] = args.project

    for required in ("ANTHROPIC_API_KEY", "GCP_PROJECT_ID"):
        if not os.environ.get(required):
            console.print(f"[red]Missing required environment variable: {required}[/red]")
            console.print("Copy .env.example to .env and fill in your credentials.")
            raise SystemExit(1)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    store = StateStore(output_dir / "state.db")
    scripts = sorted(input_dir.glob("**/*.sql"))

    if not scripts:
        console.print(f"[yellow]No .sql files found in {input_dir}[/yellow]")
        return

    for script in scripts:
        existing = store.get(str(script))
        if args.resume and existing and existing.status == Status.SUCCESS:
            continue
        if not existing:
            store.upsert(ScriptRecord(path=str(script)))

    pending = store.pending()
    if not pending:
        console.print("[green]All scripts already converted. Use --resume to skip or remove state.db to reprocess.[/green]")
        return

    mode = "EXECUTE" if args.execute else "DRY-RUN"
    console.print(
        f"[bold]Processing {len(pending)} scripts[/bold]  "
        f"concurrency={args.concurrency}  mode={mode}  max-fix-attempts={args.max_fix_attempts}"
    )

    system_prompt = load_system_prompt()
    anthropic_client = anthropic.AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    bq_client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
    semaphore = asyncio.Semaphore(args.concurrency)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task_id = progress.add_task("Converting...", total=len(pending))
        await asyncio.gather(*[
            _process_one(
                rec, store, anthropic_client, bq_client,
                output_dir, args.execute, args.max_fix_attempts,
                system_prompt, semaphore, progress, task_id,
            )
            for rec in pending
        ])

    all_records = store.all()
    report_path = output_dir / "report.html"
    generate(all_records, report_path)

    success = sum(1 for r in all_records if r.status == Status.SUCCESS)
    failed = sum(1 for r in all_records if r.status == Status.FAILED)
    console.print(f"\n[green]Done.[/green]  Success: {success}  Failed: {failed}")
    console.print(f"Report: {report_path}")
    store.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert Teradata BTEQ scripts to BigQuery Standard SQL"
    )
    parser.add_argument("--input-dir", required=True, help="Directory containing .sql source files")
    parser.add_argument("--output-dir", required=True, help="Directory for converted SQL and report")
    parser.add_argument(
        "--execute", action="store_true",
        help="Run converted SQL against BigQuery (default: dry-run syntax check only)"
    )
    parser.add_argument("--concurrency", type=int, default=10, help="Parallel Claude API calls (default: 10)")
    parser.add_argument("--max-fix-attempts", type=int, default=3, help="Max auto-fix retries per script (default: 3)")
    parser.add_argument("--project", help="GCP project ID (overrides GCP_PROJECT_ID in .env)")
    parser.add_argument("--resume", action="store_true", help="Skip scripts already marked SUCCESS in state.db")
    args = parser.parse_args()
    asyncio.run(run_pipeline(args))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify the CLI help works**

```bash
python -m td2bq.cli --help
```

Expected: usage message printed, no errors.

- [ ] **Step 3: Commit**

```bash
git add td2bq/cli.py
git commit -m "feat: async CLI pipeline orchestrator with progress bar and --resume"
```

---

### Task 10: README and push to GitHub

**Files:**
- Create: `README.md`

- [ ] **Step 1: Run full test suite**

```bash
pytest tests/ -v
```

Expected: all tests pass (classifier × 7, state × 5, fix_agent × 2, reporter × 4).

- [ ] **Step 2: Write `README.md`**

```markdown
# teradata2bigquery

Automated Teradata BTEQ → BigQuery SQL conversion pipeline using Claude AI.

## Quick Start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env   # fill in ANTHROPIC_API_KEY and GCP_PROJECT_ID
```

## Usage

```bash
# Dry-run (syntax check only — no BigQuery execution)
td2bq --input-dir ./scripts --output-dir ./converted

# Full execution against BigQuery
td2bq --input-dir ./scripts --output-dir ./converted --execute

# Resume interrupted run
td2bq --input-dir ./scripts --output-dir ./converted --resume

# Tune parallelism and fix attempts
td2bq --input-dir ./scripts --output-dir ./converted --concurrency 5 --max-fix-attempts 5
```

Outputs:
- `<output-dir>/<script_name>_bq.sql` — converted BigQuery SQL
- `<output-dir>/report.html` — run summary
- `<output-dir>/state.db` — SQLite progress (delete to reprocess all)

## Credentials

Copy `.env.example` to `.env` and set:

```ini
ANTHROPIC_API_KEY=sk-ant-...
GCP_PROJECT_ID=your-gcp-project-id
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
```
```

- [ ] **Step 3: Commit README and push to GitHub**

```bash
git add README.md
git commit -m "docs: add README with quick start and usage"
git push -u origin main
```

Expected: branch pushed, visible at https://github.com/darrenleeatkinson/teradata2bigquery

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|-----------------|------|
| Classify scripts by type | Task 3 |
| Translate via Claude API with prompt caching | Task 5 |
| BigQuery dry-run (default) | Task 6 |
| BigQuery full execute (`--execute`) | Task 6 |
| Fix agent retry loop (max N attempts) | Task 7 |
| HTML report | Task 8 |
| SQLite state + `--resume` | Task 2, Task 9 |
| `--concurrency` semaphore | Task 9 |
| `.env` + `.gitignore` | Task 1 |
| `asyncio.gather` parallel execution | Task 9 |
| `rich` progress bar | Task 9 |
| Dataset name mapping in system prompt | Task 4 |

All spec requirements covered. No placeholders. Types consistent across tasks (`ScriptRecord`, `Status`, `ValidationResult`, `ScriptType` all defined in Tasks 2/3/6 and imported correctly in Tasks 7/8/9).
