import tempfile
from pathlib import Path

from td2bq.state import ScriptRecord, StateStore, Status


def _store() -> StateStore:
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
