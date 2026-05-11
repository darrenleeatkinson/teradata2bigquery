import argparse
import asyncio
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn

from .classifier import classify
from .fix_agent import fix
from .providers import LLMProvider, create_provider
from .reporter import generate
from .state import ScriptRecord, StateStore, Status
from .translator import load_system_prompt, translate
from .validator import validate

console = Console()

_PROVIDER_ENV_KEYS = {
    "anthropic": "ANTHROPIC_API_KEY",
    "gemini": "GOOGLE_API_KEY",
}


async def _process_one(
    record: ScriptRecord,
    store: StateStore,
    provider: LLMProvider,
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
            bq_sql = await translate(sql, script_type, provider, project_id)
            result = validate(bq_sql, bq_client, execute)
            attempts = 1

            while not result.ok and attempts < max_fix_attempts:
                bq_sql = await fix(sql, bq_sql, result, provider, system_prompt)
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

    required_llm_key = _PROVIDER_ENV_KEYS.get(args.provider)
    for required in (required_llm_key, "GCP_PROJECT_ID"):
        if required and not os.environ.get(required):
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
        console.print(
            "[green]All scripts already converted. "
            "Remove state.db to reprocess all, or omit --resume to retry failures.[/green]"
        )
        return

    mode = "EXECUTE" if args.execute else "DRY-RUN"
    console.print(
        f"[bold]Processing {len(pending)} scripts[/bold]  "
        f"provider={args.provider}  model={args.model or 'default'}  "
        f"concurrency={args.concurrency}  mode={mode}  max-fix-attempts={args.max_fix_attempts}"
    )

    provider = create_provider(args.provider, model=args.model)
    system_prompt = load_system_prompt()
    bq_client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
    semaphore = asyncio.Semaphore(args.concurrency)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task_id = progress.add_task(
            f"Converting via {provider.model_name}...", total=len(pending)
        )
        await asyncio.gather(*[
            _process_one(
                rec, store, provider, bq_client,
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
        "--provider", default="anthropic", choices=["anthropic", "gemini"],
        help="LLM provider to use (default: anthropic)",
    )
    parser.add_argument(
        "--model", default=None,
        help="Model name override — e.g. claude-opus-4-5, gemini-2.0-flash (default: provider default)",
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Run converted SQL against BigQuery (default: dry-run syntax check only)",
    )
    parser.add_argument("--concurrency", type=int, default=10, help="Parallel LLM API calls (default: 10)")
    parser.add_argument("--max-fix-attempts", type=int, default=3, help="Max auto-fix retries per script (default: 3)")
    parser.add_argument("--project", help="GCP project ID (overrides GCP_PROJECT_ID in .env)")
    parser.add_argument("--resume", action="store_true", help="Skip scripts already marked SUCCESS in state.db")
    args = parser.parse_args()
    asyncio.run(run_pipeline(args))


if __name__ == "__main__":
    main()
