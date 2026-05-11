# teradata2bigquery

Automated Teradata BTEQ → BigQuery SQL conversion pipeline powered by Claude or Gemini AI.

Classifies each script, translates it via an LLM (with prompt caching), validates the output against BigQuery, and auto-fixes errors in a retry loop. Produces an HTML report and supports resuming interrupted runs.

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

---

## Providers

The pipeline supports swapping the LLM backend via `--provider` and `--model`.

### Anthropic (default)

Uses Claude. No extra install needed.

```bash
td2bq --input-dir ./scripts --output-dir ./out
# equivalent to:
td2bq --input-dir ./scripts --output-dir ./out --provider anthropic

# Specific model:
td2bq --input-dir ./scripts --output-dir ./out \
      --provider anthropic --model claude-haiku-4-5-20251001
```

Default model: `claude-opus-4-5`

Required in `.env`:
```ini
ANTHROPIC_API_KEY=sk-ant-...
```

### Gemini

Uses Google Gemini. Requires the `google-genai` SDK:

```bash
pip install -e ".[gemini]"
```

```bash
td2bq --input-dir ./scripts --output-dir ./out --provider gemini

# Specific model:
td2bq --input-dir ./scripts --output-dir ./out \
      --provider gemini --model gemini-1.5-pro
```

Default model: `gemini-2.0-flash`

Required in `.env`:
```ini
GOOGLE_API_KEY=AIza...
```

---

## Credentials

Copy `.env.example` to `.env` and populate the keys for the provider(s) you use:

```ini
# Anthropic (default provider)
ANTHROPIC_API_KEY=sk-ant-...

# Gemini provider (--provider gemini)
GOOGLE_API_KEY=AIza...

# BigQuery (required for all providers)
GCP_PROJECT_ID=your-gcp-project-id
GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
```

`.env` is git-ignored and never committed.

---

## Architecture

```
input .sql files
      │
      ▼
  classifier  ──► ScriptType (VOLATILE_LOAD, UPSERT, MERGE, DDL, REPORTING, UNKNOWN)
      │
      ▼
  translator  ──► LLMProvider.complete()
      │             ├── AnthropicProvider  (cache_control: ephemeral on system prompt)
      │             └── GeminiProvider     (system_instruction via GenerateContentConfig)
      ▼
  validator   ──► BigQuery dry-run or execute
      │ (on failure)
      ▼
  fix_agent   ──► LLMProvider.complete() with error context  ──► re-validate  (up to N times)
      │
      ▼
  reporter    ──► report.html
```

Concurrency is controlled by `asyncio.Semaphore(--concurrency)`. State is persisted in SQLite — safe to Ctrl-C and `--resume`.

---

## Development

```bash
pip install -e ".[dev]"

# Run tests (note: google-cloud-bigquery is slow to import on WSL — allow ~2min)
pytest tests/test_classifier.py tests/test_state.py tests/test_reporter.py -v
pytest tests/test_fix_agent.py tests/test_providers.py -v
```
