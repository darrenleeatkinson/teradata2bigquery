# teradata2bigquery

Automated Teradata BTEQ → BigQuery SQL conversion pipeline powered by Claude AI.

Classifies each script, translates it via Claude API (with prompt caching), validates the output against BigQuery, and auto-fixes errors in a retry loop. Produces an HTML report and supports resuming interrupted runs.

## Quick Start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env   # fill in your credentials
```

## Usage

```bash
# Dry-run: translate and syntax-check only (no BigQuery execution)
td2bq --input-dir ./scripts --output-dir ./converted

# Full execution: run translated SQL against BigQuery
td2bq --input-dir ./scripts --output-dir ./converted --execute

# Resume an interrupted run (skip already-successful scripts)
td2bq --input-dir ./scripts --output-dir ./converted --resume

# Tune concurrency and fix attempts
td2bq --input-dir ./scripts --output-dir ./converted \
      --concurrency 5 --max-fix-attempts 5

# Override GCP project (instead of .env)
td2bq --input-dir ./scripts --output-dir ./converted --project my-gcp-project
```

### Output

| File | Description |
|------|-------------|
| `<output-dir>/<name>_bq.sql` | Converted BigQuery SQL |
| `<output-dir>/report.html` | Run summary with per-script status |
| `<output-dir>/state.db` | SQLite progress store (delete to reprocess all) |

## Credentials

Copy `.env.example` to `.env` and populate:

```ini
ANTHROPIC_API_KEY=sk-ant-...
GCP_PROJECT_ID=your-gcp-project-id
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
```

`.env` is git-ignored and never committed.

## Architecture

```
input .sql files
      │
      ▼
  classifier  ──► ScriptType (VOLATILE_LOAD, UPSERT, MERGE, DDL, REPORTING, UNKNOWN)
      │
      ▼
  translator  ──► Claude API (system prompt cached via cache_control: ephemeral)
      │
      ▼
  validator   ──► BigQuery dry-run or execute
      │ (on failure)
      ▼
  fix_agent   ──► Claude API with error context  ──► re-validate  (up to N times)
      │
      ▼
  reporter    ──► report.html
```

Concurrency is controlled by `asyncio.Semaphore(--concurrency)`. State is persisted in SQLite — safe to Ctrl-C and resume.

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v
```
