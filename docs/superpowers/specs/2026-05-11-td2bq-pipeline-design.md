# Teradata → BigQuery Automated Conversion Pipeline — Design Spec

**Date:** 2026-05-11  
**Author:** Darren Atkinson  
**Status:** Approved

---

## 1. Purpose

Bulk-convert thousands of Teradata BTEQ SQL scripts to BigQuery Standard SQL using an AI-powered pipeline. The pipeline:

- Classifies each script by type (volatile table load, MERGE/upsert, DDL, reporting, etc.)
- Translates it via Claude API with prompt caching
- Validates the output against BigQuery (dry-run syntax check, or full execution with `--execute`)
- Auto-fixes errors in up to N retry loops
- Produces a per-run HTML report

---

## 2. Architecture: Async Zero-Infra Pipeline

Chosen approach: Python `asyncio` with semaphore-controlled concurrency (Option C).

```
input_dir/
  *.sql  (Teradata BTEQ scripts)

pipeline
  ├── classifier       — regex + heuristic script type detection
  ├── translator       — Claude API (prompt-cached system prompt)
  ├── validator        — BigQuery dry-run or execute
  ├── fix_agent        — retry loop (up to --max-fix-attempts)
  └── reporter         — HTML + JSON summary

output_dir/
  <script_name>_bq.sql     — converted BigQuery SQL
  report.html              — run report
  state.db                 — SQLite progress (supports --resume)
```

**Concurrency model:** `asyncio.Semaphore(concurrency)` limits parallel Claude API calls. Default concurrency = 10.

**Prompt caching:** The large system prompt (Teradata→BQ translation rules + mappings) is sent once with `cache_control: {"type": "ephemeral"}`. All subsequent calls in the same run share the cached prefix — reduces cost and latency ~80% after the first call.

---

## 3. Components

### 3.1 CLI (`cli.py`)

```
python -m td2bq convert \
  --input-dir  ./scripts \
  --output-dir ./converted \
  [--execute]              # full BQ execution; default is dry-run only
  [--concurrency 10]       # parallel Claude API calls
  [--max-fix-attempts 3]   # retry loop limit per script
  [--project GCP_PROJECT]  # override .env
  [--resume]               # skip already-completed scripts
```

### 3.2 Classifier

Inspects script text for patterns:

| Pattern | ScriptType |
|---------|-----------|
| `CREATE VOLATILE` / `INSERT INTO wt_` | `VOLATILE_LOAD` |
| `UPDATE TGT FROM` + `INSERT INTO ... SELECT` | `UPSERT` |
| `CREATE TABLE` (permanent) | `DDL` |
| `MERGE INTO` | `MERGE` |
| `SELECT` only (no DML) | `REPORTING` |
| Other | `UNKNOWN` |

### 3.3 Translator

Sends each script to Claude API with:

- **System prompt** (cached): Full Teradata→BQ rule set from the `teradata-to-bigquery` skill, including all mappings, patterns, dataset name substitutions, and the RI sentinel convention.
- **User message**: The raw BTEQ script + metadata (script type, dataset hints).
- Returns: Translated BigQuery SQL string.

Prompt structure per call:
```
[CACHED] system: <full translation rules — ~4000 tokens>
user: Convert this Teradata script. Type: {script_type}. Script: {sql}
```

### 3.4 Validator

Two modes:

- **Dry-run** (default): `google.cloud.bigquery.Client.query(sql, job_config=QueryJobConfig(dry_run=True))`. Catches syntax errors without executing.
- **Full execute** (`--execute`): Runs the script. Captures `@@row_count` results and any runtime errors.

Returns: `ValidationResult(ok: bool, errors: list[str], rows_affected: int | None)`.

### 3.5 Fix Agent

If validation fails:
1. Send original script + BigQuery error message back to Claude
2. Request targeted fix
3. Re-validate
4. Repeat up to `--max-fix-attempts` (default 3)

If still failing after max attempts: mark as `FAILED`, save last attempt to output dir with error notes.

### 3.6 State Store (`state.db`)

SQLite with one table:

```sql
CREATE TABLE scripts (
  path         TEXT PRIMARY KEY,
  script_type  TEXT,
  status       TEXT,   -- pending|in_progress|success|failed
  attempts     INTEGER,
  error        TEXT,
  output_path  TEXT,
  translated_at TIMESTAMP
);
```

`--resume` skips rows where `status = 'success'`.

### 3.7 Reporter

Generates `report.html` at run end:

- Summary stats: total, success, failed, skipped
- Per-script table: name, type, status, attempts, error snippet
- Colour-coded (green/amber/red)
- Self-contained single HTML file (no external dependencies)

---

## 4. Configuration (`.env`)

```ini
ANTHROPIC_API_KEY=sk-ant-...
GCP_PROJECT_ID=your-gcp-project
GCP_DATASET_PREFIX=             # optional: prepend to all dataset names
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
```

`.env.example` committed to repo. `.env` in `.gitignore`.

---

## 5. Dataset Name Mapping

Applied at translation time by the system prompt:

| Teradata Database | BigQuery Dataset |
|-------------------|-----------------|
| `P_BASE_LOAD` | `base_load` |
| `P_INTEGRATION_VIEWS` | `integration` |
| `P_INTEGRATION_DAILY_TABLES` | `integration_daily` |
| `P_INTEGRATION_REFERENCE_TABLES` | `reference` |
| `P_SUBSCRIBER` | `subscriber` |
| `P_CUSTOMER` | `customer` |
| `P_REFERENCE` | `ref_data` |
| `P_USER_VIEWS` | `user_views` |

---

## 6. Project Layout

```
teradata2bigquery/
├── td2bq/
│   ├── __init__.py
│   ├── cli.py              # argparse entry point
│   ├── classifier.py       # ScriptType detection
│   ├── translator.py       # Claude API + prompt caching
│   ├── validator.py        # BigQuery dry-run / execute
│   ├── fix_agent.py        # error-retry loop
│   ├── reporter.py         # HTML report
│   ├── state.py            # SQLite state store
│   └── prompts/
│       └── system.md       # Translation system prompt (cached)
├── docs/
│   └── superpowers/
│       └── specs/
│           └── 2026-05-11-td2bq-pipeline-design.md
├── tests/
│   └── test_classifier.py
├── .env.example
├── .gitignore
├── pyproject.toml
└── README.md
```

---

## 7. Dependencies

```toml
[project]
dependencies = [
  "anthropic>=0.25",
  "google-cloud-bigquery>=3.11",
  "python-dotenv>=1.0",
  "jinja2>=3.1",         # HTML report template
  "rich>=13",            # CLI progress output
]
```

Python ≥ 3.11 (uses `asyncio.TaskGroup`).

---

## 8. Error Handling

- Network errors (Claude API / BQ): exponential backoff, 3 retries, then mark FAILED
- BQ auth errors: surface immediately with clear message pointing to `.env`
- Script encoding issues: read with `errors='replace'`, log warning
- Partial runs: SQLite state means safe to Ctrl-C and `--resume`

---

## 9. Success Criteria

- All scripts classified correctly (manual spot-check sample)
- Dry-run passes for ≥90% of converted scripts on first attempt
- Fix agent resolves ≥80% of remaining failures
- HTML report accurately reflects final state
- `--resume` correctly skips already-successful scripts

---

## 10. Out of Scope

- Test data generation (future phase)
- Semantic diff / row-count comparison between TD and BQ output
- Web UI
- Horizontal scaling (single-machine asyncio is sufficient)
