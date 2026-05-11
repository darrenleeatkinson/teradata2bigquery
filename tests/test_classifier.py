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
