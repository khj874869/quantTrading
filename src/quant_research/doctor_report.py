from __future__ import annotations

import html


def render_doctor_html_report(summary: dict[str, object]) -> str:
    checks = summary.get("checks", [])
    if not isinstance(checks, list):
        checks = []
    rows = "\n".join(_html_check_row(check) for check in checks)
    status = html.escape(str(summary.get("status", "unknown")))
    generated_at = html.escape(str(summary.get("generated_at", "")))
    config_path = html.escape(str(summary.get("config_path", "")))
    fail_count = html.escape(str(summary.get("fail_count", 0)))
    warn_count = html.escape(str(summary.get("warn_count", 0)))
    pass_count = html.escape(str(summary.get("pass_count", 0)))
    check_count = html.escape(str(summary.get("check_count", len(checks))))
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Doctor Report</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f7f8fa;
      --panel: #ffffff;
      --text: #17202a;
      --muted: #5d6875;
      --border: #d9dee7;
      --pass: #16794c;
      --warn: #a45f00;
      --fail: #b42318;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: Arial, Helvetica, sans-serif;
      font-size: 14px;
      line-height: 1.5;
    }}
    main {{
      width: min(1180px, calc(100% - 32px));
      margin: 28px auto 40px;
    }}
    header {{
      display: flex;
      justify-content: space-between;
      gap: 20px;
      align-items: flex-start;
      margin-bottom: 18px;
    }}
    h1 {{
      margin: 0 0 6px;
      font-size: 28px;
      line-height: 1.15;
    }}
    .meta {{
      color: var(--muted);
      overflow-wrap: anywhere;
    }}
    .status {{
      min-width: 128px;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--panel);
      padding: 12px 14px;
      text-align: center;
    }}
    .status strong {{
      display: block;
      font-size: 22px;
      text-transform: uppercase;
    }}
    .summary {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }}
    .metric {{
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--panel);
      padding: 14px;
    }}
    .metric span {{
      display: block;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
    }}
    .metric strong {{
      display: block;
      margin-top: 6px;
      font-size: 24px;
    }}
    .filters {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 0 0 14px;
    }}
    .filter-button {{
      appearance: none;
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--panel);
      color: var(--text);
      cursor: pointer;
      font: inherit;
      font-weight: 700;
      padding: 7px 12px;
    }}
    .filter-button[aria-pressed="true"] {{
      border-color: #17202a;
      box-shadow: inset 0 0 0 1px #17202a;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: hidden;
    }}
    th, td {{
      padding: 10px 12px;
      border-bottom: 1px solid var(--border);
      text-align: left;
      vertical-align: top;
    }}
    th {{
      background: #eef2f7;
      font-size: 12px;
      text-transform: uppercase;
      color: var(--muted);
    }}
    tr:last-child td {{ border-bottom: 0; }}
    .badge {{
      display: inline-block;
      min-width: 54px;
      border-radius: 999px;
      padding: 2px 8px;
      color: #ffffff;
      font-size: 12px;
      font-weight: 700;
      text-align: center;
      text-transform: uppercase;
    }}
    .pass {{ background: var(--pass); }}
    .warn {{ background: var(--warn); }}
    .fail {{ background: var(--fail); }}
    .path {{
      color: var(--muted);
      overflow-wrap: anywhere;
    }}
    tr[hidden] {{ display: none; }}
    @media (max-width: 760px) {{
      header {{ display: block; }}
      .status {{ margin-top: 14px; text-align: left; }}
      .summary {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      table {{ display: block; overflow-x: auto; }}
      th, td {{ min-width: 120px; }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>Doctor Report</h1>
        <div class="meta">Generated {generated_at}</div>
        <div class="meta">Config {config_path}</div>
      </div>
      <div class="status"><span>Status</span><strong>{status}</strong></div>
    </header>
    <section class="summary">
      <div class="metric"><span>Checks</span><strong>{check_count}</strong></div>
      <div class="metric"><span>Passed</span><strong>{pass_count}</strong></div>
      <div class="metric"><span>Warnings</span><strong>{warn_count}</strong></div>
      <div class="metric"><span>Failures</span><strong>{fail_count}</strong></div>
    </section>
    <nav class="filters" aria-label="Check status filter">
      <button class="filter-button" type="button" data-filter="all" aria-pressed="true">All</button>
      <button class="filter-button" type="button" data-filter="fail" aria-pressed="false">Fail</button>
      <button class="filter-button" type="button" data-filter="warn" aria-pressed="false">Warn</button>
      <button class="filter-button" type="button" data-filter="pass" aria-pressed="false">Pass</button>
    </nav>
    <table>
      <thead>
        <tr>
          <th>Status</th>
          <th>Severity</th>
          <th>Category</th>
          <th>Name</th>
          <th>Message</th>
          <th>Path</th>
        </tr>
      </thead>
      <tbody>
        {rows}
      </tbody>
    </table>
  </main>
  <script>
    const filterButtons = document.querySelectorAll("[data-filter]");
    const checkRows = document.querySelectorAll("tbody tr[data-status]");
    for (const button of filterButtons) {{
      button.addEventListener("click", () => {{
        const filter = button.dataset.filter || "all";
        for (const candidate of filterButtons) {{
          candidate.setAttribute("aria-pressed", String(candidate === button));
        }}
        for (const row of checkRows) {{
          row.hidden = filter !== "all" && row.dataset.status !== filter;
        }}
      }});
    }}
  </script>
</body>
</html>
"""


def _html_check_row(check: object) -> str:
    if not isinstance(check, dict):
        check = {}
    status = str(check.get("status", "unknown"))
    status_class = status if status in {"pass", "warn", "fail"} else "warn"
    return (
        f'<tr data-status="{html.escape(status_class)}">'
        f'<td><span class="badge {html.escape(status_class)}">{html.escape(status)}</span></td>'
        f"<td>{html.escape(str(check.get('severity', '')))}</td>"
        f"<td>{html.escape(str(check.get('category', '')))}</td>"
        f"<td>{html.escape(str(check.get('name', '')))}</td>"
        f"<td>{html.escape(str(check.get('message', '')))}</td>"
        f"<td class=\"path\">{html.escape(str(check.get('path') or ''))}</td>"
        "</tr>"
    )
