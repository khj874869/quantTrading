from __future__ import annotations

import html
import json
import shutil
from pathlib import Path

from .config import Config
from .utils import ensure_directory


class DemoPublisher:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.output_dir = config.resolve_path(config.paths.get("output_dir", "output"))
        self.demo_dir = config.resolve_path(config.paths.get("demo_site_dir", "docs/demo"))

    def publish(self) -> tuple[list[Path], dict[str, object]]:
        ensure_directory(self.demo_dir)
        nojekyll_path = self.demo_dir / ".nojekyll"
        nojekyll_path.write_text("", encoding="utf-8")
        report_summary = self._read_json(self.output_dir / "report_summary.json")
        execution_summary = self._read_json(self.output_dir / "execution_summary.json", allow_missing=True)
        run_manifest = self._read_json(self.output_dir / "run_manifest.json", allow_missing=True)
        gallery_summary = self._read_json(self.demo_dir / "gallery_summary.json", allow_missing=True)
        latest_winner = self._read_json(self.demo_dir / "latest_winner.json", allow_missing=True)

        copied_outputs = []
        file_map = [
            ("report_dashboard.html", "report_dashboard.html"),
            ("report_summary.json", "report_summary.json"),
            ("report_monthly_returns.csv", "report_monthly_returns.csv"),
            ("report_capacity_curve.csv", "report_capacity_curve.csv"),
            ("report_factor_diagnostics.csv", "report_factor_diagnostics.csv"),
            ("report_stress_scenarios.csv", "report_stress_scenarios.csv"),
            ("execution_summary.json", "execution_summary.json"),
            ("execution_reconciliation.csv", "execution_reconciliation.csv"),
            ("order_blotter_latest.csv", "order_blotter_latest.csv"),
            ("universe_snapshot.csv", "universe_snapshot.csv"),
            ("run_manifest.json", "run_manifest.json"),
        ]
        for source_name, target_name in file_map:
            source_path = self.output_dir / source_name
            if not source_path.exists():
                if source_name in {"report_dashboard.html", "report_summary.json"}:
                    raise FileNotFoundError(f"missing required output for demo publishing: {source_path}")
                continue
            target_path = self.demo_dir / target_name
            shutil.copy2(source_path, target_path)
            copied_outputs.append(target_path)

        summary = self._build_bundle_summary(report_summary, execution_summary, run_manifest, copied_outputs)
        summary_path = self.demo_dir / "bundle_summary.json"
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        copied_outputs.append(summary_path)

        index_path = self.demo_dir / "index.html"
        index_path.write_text(
            self._build_index_html(summary, copied_outputs, gallery_summary, latest_winner),
            encoding="utf-8",
        )
        copied_outputs.append(index_path)
        copied_outputs.append(nojekyll_path)
        return copied_outputs, summary

    def _build_bundle_summary(
        self,
        report_summary: dict[str, object],
        execution_summary: dict[str, object],
        run_manifest: dict[str, object],
        copied_outputs: list[Path],
    ) -> dict[str, object]:
        backtest_summary = report_summary.get("backtest_summary", {})
        projected_files = [path.name for path in copied_outputs] + ["bundle_summary.json", "index.html", ".nojekyll"]
        return {
            "demo_site_dir": str(self.demo_dir),
            "artifact_count": len(projected_files),
            "headline_metrics": {
                "net_total_return": float(backtest_summary.get("net_total_return", 0.0)),
                "sharpe": float(backtest_summary.get("sharpe", 0.0)),
                "max_drawdown": float(backtest_summary.get("max_drawdown", 0.0)),
                "largest_aum_without_breach": report_summary.get("largest_aum_without_breach"),
                "execution_cost_bps_vs_filled_notional": float(execution_summary.get("execution_cost_bps_vs_filled_notional", 0.0)),
            },
            "best_month": report_summary.get("best_month"),
            "worst_month": report_summary.get("worst_month"),
            "top_factor": (report_summary.get("top_factors_by_ic") or [None])[0],
            "top_security": (report_summary.get("top_securities") or [None])[0],
            "top_sector": (report_summary.get("top_sectors") or [None])[0],
            "run_manifest_generated_at": run_manifest.get("generated_at"),
            "files": projected_files,
        }

    def _read_json(self, path: Path, allow_missing: bool = False) -> dict[str, object]:
        if not path.exists():
            if allow_missing:
                return {}
            raise FileNotFoundError(f"missing required output: {path}")
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict):
            return payload
        return {}

    def _build_index_html(
        self,
        summary: dict[str, object],
        copied_outputs: list[Path],
        gallery_summary: dict[str, object],
        latest_winner: dict[str, object],
    ) -> str:
        metrics = summary.get("headline_metrics", {})
        cards = [
            ("Net Return", self._format_pct(metrics.get("net_total_return", 0.0))),
            ("Sharpe", self._format_number(metrics.get("sharpe", 0.0))),
            ("Max Drawdown", self._format_pct(metrics.get("max_drawdown", 0.0))),
            ("Capacity", self._format_currency(metrics.get("largest_aum_without_breach"))),
            ("Execution Cost", self._format_bps(metrics.get("execution_cost_bps_vs_filled_notional", 0.0))),
        ]
        card_html = "".join(
            f"""
            <article class="metric">
              <div class="label">{html.escape(label)}</div>
              <div class="value">{html.escape(value)}</div>
            </article>
            """
            for label, value in cards
        )
        file_links = "".join(
            f'<li><a href="{html.escape(path.name)}">{html.escape(path.name)}</a></li>'
            for path in copied_outputs
            if path.name != "index.html"
        )
        best_month = summary.get("best_month") or {}
        worst_month = summary.get("worst_month") or {}
        top_factor = summary.get("top_factor") or {}
        top_security = summary.get("top_security") or {}
        top_sector = summary.get("top_sector") or {}
        spotlight_html = self._build_gallery_spotlight(gallery_summary, latest_winner)
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Quant Demo Bundle</title>
  <style>
    :root {{
      --bg: #f2ede4;
      --panel: rgba(255, 250, 242, 0.9);
      --panel-strong: #fffaf3;
      --ink: #171411;
      --muted: #655c53;
      --line: rgba(68, 49, 29, 0.12);
      --accent: #ab4b1b;
      --cool: #215364;
      --shadow: 0 22px 60px rgba(54, 33, 13, 0.14);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(171, 75, 27, 0.16), transparent 28%),
        radial-gradient(circle at top right, rgba(33, 83, 100, 0.12), transparent 24%),
        linear-gradient(180deg, #f8f3eb 0%, var(--bg) 100%);
    }}
    .shell {{
      width: min(1180px, calc(100% - 32px));
      margin: 0 auto;
      padding: 28px 0 42px;
    }}
    .hero, .section {{
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 28px;
      box-shadow: var(--shadow);
    }}
    .hero {{ padding: 28px; }}
    .eyebrow {{
      text-transform: uppercase;
      letter-spacing: 0.16em;
      font-size: 12px;
      color: var(--accent);
      margin-bottom: 10px;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(34px, 6vw, 64px);
      line-height: 0.94;
      max-width: 780px;
    }}
    .lede {{
      margin: 14px 0 0;
      max-width: 780px;
      color: var(--muted);
      font-size: 18px;
      line-height: 1.65;
    }}
    .metric-grid {{
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      margin-top: 22px;
    }}
    .metric {{
      background: rgba(255,255,255,0.68);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px;
    }}
    .label {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .value {{
      margin-top: 10px;
      font-size: 28px;
    }}
    .section {{
      margin-top: 20px;
      padding: 22px;
    }}
    .split {{
      display: grid;
      grid-template-columns: 0.95fr 1.05fr;
      gap: 16px;
    }}
    .panel {{
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 16px;
      background: var(--panel-strong);
    }}
    .mini-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }}
    .mini {{
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px;
      background: rgba(255,255,255,0.72);
    }}
    h2, h3 {{ margin: 0 0 12px; }}
    p, li {{
      color: var(--muted);
      font-size: 15px;
      line-height: 1.6;
    }}
    ul {{
      margin: 0;
      padding-left: 18px;
      columns: 2;
    }}
    a {{
      color: var(--cool);
      text-decoration: none;
    }}
    a:hover {{ text-decoration: underline; }}
    iframe {{
      width: 100%;
      min-height: 760px;
      border: 1px solid var(--line);
      border-radius: 20px;
      background: white;
    }}
    .cta {{
      display: inline-block;
      margin-top: 14px;
      padding: 10px 14px;
      border-radius: 999px;
      background: var(--accent);
      color: white;
    }}
    @media (max-width: 980px) {{
      .metric-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .split {{ grid-template-columns: 1fr; }}
      ul {{ columns: 1; }}
    }}
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <div class="eyebrow">GitHub Pages Demo Bundle</div>
      <h1>Static research artifact, ready to publish.</h1>
      <p class="lede">This bundle packages the latest report dashboard, key diagnostics, and machine-readable summaries into a single directory so the project can be deployed on GitHub Pages without a backend.</p>
      <div class="metric-grid">{card_html}</div>
      <a class="cta" href="report_dashboard.html">Open full dashboard</a>
      <a class="cta" style="margin-left:10px;background:rgba(33,83,100,0.10);color:var(--cool);" href="gallery.html">Open strategy gallery</a>
    </section>
    <section class="section split">
      <div class="panel">
        <h2>Highlights</h2>
        <div class="mini-grid">
          <article class="mini">
            <h3>Best Month</h3>
            <p>{html.escape(str(best_month.get("month", "-")))} | {html.escape(self._format_pct(best_month.get("net_total_return", 0.0)))}</p>
          </article>
          <article class="mini">
            <h3>Worst Month</h3>
            <p>{html.escape(str(worst_month.get("month", "-")))} | {html.escape(self._format_pct(worst_month.get("net_total_return", 0.0)))}</p>
          </article>
          <article class="mini">
            <h3>Top Factor</h3>
            <p>{html.escape(str(top_factor.get("factor", "-")))} | IC {html.escape(self._format_number(top_factor.get("average_ic", 0.0)))}</p>
          </article>
          <article class="mini">
            <h3>Top Security</h3>
            <p>{html.escape(str(top_security.get("permno", "-")))} | {html.escape(self._format_pct(top_security.get("total_contribution", 0.0)))}</p>
          </article>
          <article class="mini">
            <h3>Top Sector</h3>
            <p>{html.escape(str(top_sector.get("sector", "-")))} | {html.escape(self._format_pct(top_sector.get("total_contribution", 0.0)))}</p>
          </article>
          <article class="mini">
            <h3>Manifest Time</h3>
            <p>{html.escape(str(summary.get("run_manifest_generated_at", "-")))}</p>
          </article>
        </div>
      </div>
      <div class="panel">
        <h2>Bundle Files</h2>
        <p>These files are copied into the published demo directory and can be linked directly from a GitHub Pages site.</p>
        <ul>{file_links}</ul>
      </div>
    </section>
    {spotlight_html}
    <section class="section">
      <h2>Embedded Dashboard</h2>
      <iframe src="report_dashboard.html" title="Quant report dashboard"></iframe>
    </section>
  </main>
</body>
</html>
"""

    def _build_gallery_spotlight(self, gallery_summary: dict[str, object], latest_winner: dict[str, object]) -> str:
        presets = gallery_summary.get("presets")
        if not isinstance(presets, list) or not presets:
            return ""
        best_return = self._find_tagged_preset(presets, "best_return")
        best_sharpe = self._find_tagged_preset(presets, "best_sharpe")
        highest_capacity = self._find_tagged_preset(presets, "highest_capacity")
        lowest_turnover = self._find_tagged_preset(presets, "lowest_turnover")
        spotlight = latest_winner or best_return or best_sharpe or presets[0]
        share_card_path = html.escape(str(spotlight.get("share_card_path", "")))
        winner_badge_path = "latest_winner_badge.svg"
        winner_md_path = "latest_winner.md"
        winner_readme_path = "latest_winner_readme_snippet.md"
        winner_release_note_path = "latest_winner_release_note.md"
        winner_social_path = "latest_winner_social_post.txt"
        share_preview = (
            f"""
            <div class="panel">
              <h2>Share Card</h2>
              <p>The current spotlight preset already has a ready-to-share SVG asset plus channel-ready snippets.</p>
              <a href="{share_card_path}"><img src="{share_card_path}" alt="Preset share card" style="width:100%;border-radius:18px;border:1px solid rgba(68,49,29,0.12);background:white;"></a>
              <p style="margin-top:12px;">
                <a href="{winner_badge_path}">Open latest winner badge</a> |
                <a href="{winner_md_path}">Open winner markdown</a> |
                <a href="{winner_readme_path}">Open README snippet</a> |
                <a href="{winner_release_note_path}">Open release note</a> |
                <a href="{winner_social_path}">Open social post</a>
              </p>
            </div>
            """
            if share_card_path
            else ""
        )
        winners = [entry for entry in [best_return, best_sharpe, highest_capacity, lowest_turnover] if entry]
        winner_rows = "".join(
            f"""
            <article class="mini">
              <h3>{html.escape(", ".join(str(tag) for tag in winner.get("tags", [])) or "winner")}</h3>
              <p><a href="{html.escape(str(winner.get("report_path", "")))}">{html.escape(str(winner.get("title", "-")))}</a></p>
            </article>
            """
            for winner in winners
        )
        return f"""
    <section class="section split">
      <div class="panel">
        <h2>Preset Spotlight</h2>
        <p>The root demo now highlights the latest strongest preset from the gallery instead of staying frozen on one generic sample.</p>
        <div class="mini-grid">
          <article class="mini">
            <h3>Spotlight</h3>
            <p><a href="{html.escape(str(spotlight.get("report_path", "")))}">{html.escape(str(spotlight.get("title", "-")))}</a></p>
          </article>
          <article class="mini">
            <h3>Net Return</h3>
            <p>{html.escape(self._format_pct(spotlight.get("net_total_return", 0.0)))}</p>
          </article>
          <article class="mini">
            <h3>Sharpe</h3>
            <p>{html.escape(self._format_number(spotlight.get("sharpe", 0.0)))}</p>
          </article>
          <article class="mini">
            <h3>Capacity</h3>
            <p>{html.escape(self._format_currency(spotlight.get("largest_aum_without_breach")))}</p>
          </article>
        </div>
        <div class="mini-grid" style="margin-top:12px;">{winner_rows}</div>
        <p style="margin-top:14px;"><a href="gallery.html">Open full strategy gallery</a></p>
      </div>
      {share_preview}
    </section>
"""

    def _find_tagged_preset(self, presets: list[object], tag: str) -> dict[str, object] | None:
        for preset in presets:
            if isinstance(preset, dict) and tag in list(preset.get("tags", [])):
                return preset
        return None

    def _format_pct(self, value: object) -> str:
        return f"{float(value or 0.0) * 100.0:.2f}%"

    def _format_currency(self, value: object) -> str:
        if value in (None, ""):
            return "-"
        return f"${float(value):,.0f}"

    def _format_number(self, value: object) -> str:
        return f"{float(value or 0.0):.2f}"

    def _format_bps(self, value: object) -> str:
        return f"{float(value or 0.0):.1f} bps"
