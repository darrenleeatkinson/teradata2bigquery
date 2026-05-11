from datetime import datetime
from pathlib import Path

from jinja2 import Environment

from .state import ScriptRecord, Status

_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Teradata → BigQuery Conversion Report</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 2em; color: #333; }
  h1 { color: #1a73e8; margin-bottom: 0.25em; }
  .meta { color: #5f6368; font-size: 0.9em; margin-bottom: 1.5em; }
  .summary { display: flex; gap: 1.5em; margin: 1.5em 0; flex-wrap: wrap; }
  .stat { background: #f1f3f4; padding: 1em 2em; border-radius: 8px; text-align: center; min-width: 90px; }
  .stat .n { font-size: 2.5em; font-weight: 700; line-height: 1.1; }
  .c-success { color: #34a853; }
  .c-failed  { color: #ea4335; }
  .c-pending { color: #fbbc04; }
  table { width: 100%; border-collapse: collapse; margin-top: 1em; font-size: 0.88em; }
  th { background: #1a73e8; color: white; padding: 0.6em 1em; text-align: left; font-weight: 500; }
  td { padding: 0.5em 1em; border-bottom: 1px solid #e8eaed; vertical-align: top; }
  tr:nth-child(even) td { background: #f8f9fa; }
  tr:hover td { background: #e8f0fe; }
  .badge { padding: 2px 10px; border-radius: 12px; font-size: 0.8em; font-weight: 600; white-space: nowrap; }
  .badge-success     { background: #e6f4ea; color: #137333; }
  .badge-failed      { background: #fce8e6; color: #c5221f; }
  .badge-pending     { background: #fef7e0; color: #b06000; }
  .badge-in_progress { background: #e8f0fe; color: #1967d2; }
  .err { font-family: 'Courier New', monospace; font-size: 0.78em; color: #c5221f;
         white-space: pre-wrap; word-break: break-all; max-width: 500px; }
</style>
</head>
<body>
<h1>Teradata → BigQuery Conversion Report</h1>
<div class="meta">Generated: {{ generated_at }}</div>

<div class="summary">
  <div class="stat"><div class="n">{{ total }}</div><div>Total</div></div>
  <div class="stat"><div class="n c-success">{{ success }}</div><div>Success</div></div>
  <div class="stat"><div class="n c-failed">{{ failed }}</div><div>Failed</div></div>
  <div class="stat"><div class="n c-pending">{{ pending }}</div><div>Pending / In progress</div></div>
</div>

<table>
  <thead>
    <tr>
      <th>Script</th>
      <th>Type</th>
      <th>Status</th>
      <th>Attempts</th>
      <th>Error</th>
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

    env = Environment()
    env.filters["basename"] = lambda p: Path(p).name
    t = env.from_string(_TEMPLATE)

    html = t.render(
        generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        total=total,
        success=success,
        failed=failed,
        pending=pending,
        records=records,
    )
    output_path.write_text(html, encoding="utf-8")
