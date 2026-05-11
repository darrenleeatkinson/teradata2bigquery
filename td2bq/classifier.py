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
