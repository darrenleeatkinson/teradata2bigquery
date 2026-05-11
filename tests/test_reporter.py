import tempfile
from pathlib import Path

from td2bq.reporter import generate
from td2bq.state import ScriptRecord, Status


def _records() -> list[ScriptRecord]:
    return [
        ScriptRecord(path="/scripts/a.sql", script_type="VOLATILE_LOAD", status=Status.SUCCESS, attempts=1),
        ScriptRecord(path="/scripts/b.sql", script_type="UPSERT", status=Status.FAILED, attempts=3, error="Syntax error at line 5"),
        ScriptRecord(path="/scripts/c.sql", script_type="UNKNOWN", status=Status.PENDING, attempts=0),
    ]


def test_generate_creates_html_file():
    out = Path(tempfile.mktemp(suffix=".html"))
    generate(_records(), out)
    assert out.exists()
    content = out.read_text()
    assert "<html" in content


def test_report_contains_total_count():
    out = Path(tempfile.mktemp(suffix=".html"))
    generate(_records(), out)
    content = out.read_text()
    assert ">3<" in content


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
    assert "Syntax error at line 5" in out.read_text()
