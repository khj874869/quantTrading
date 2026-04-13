from __future__ import annotations

import copy
import html
import json
from datetime import datetime
from pathlib import Path

from .backtest import Backtester
from .config import Config
from .pipeline import PreparedData
from .publishing import DemoPublisher
from .reporting import PerformanceReporter
from .strategy import MultiSignalStrategy
from .utils import ensure_directory


DEFAULT_GALLERY_PRESETS = [
    {
        "slug": "quality_defensive",
        "title": "Quality Defensive",
        "description": "Lower turnover, tighter position caps, optimizer on.",
        "overrides": {
            "portfolio_construction": "optimizer",
            "optimizer_risk_aversion": 0.8,
            "optimizer_turnover_penalty": 0.45,
            "max_turnover_per_rebalance": 0.18,
            "max_position_weight": 0.12,
            "entry_score_threshold": 0.12,
        },
    },
    {
        "slug": "revision_momentum",
        "title": "Revision Momentum",
        "description": "Faster entry, looser turnover budget, more aggressive participation.",
        "overrides": {
            "portfolio_construction": "optimizer",
            "optimizer_risk_aversion": 0.35,
            "optimizer_turnover_penalty": 0.12,
            "max_turnover_per_rebalance": 0.4,
            "max_position_weight": 0.22,
            "entry_score_threshold": 0.0,
            "incumbent_score_bonus": 0.0,
        },
    },
    {
        "slug": "market_neutral_ls",
        "title": "Market Neutral LS",
        "description": "Long-short preset with borrow cost and tighter gross risk controls.",
        "overrides": {
            "portfolio_construction": "optimizer",
            "long_short": True,
            "holding_count": 2,
            "benchmark_hedge": False,
            "beta_neutral": True,
            "sector_neutral": False,
            "short_borrow_cost_bps_annual": 75.0,
            "max_position_weight": 0.15,
            "optimizer_risk_aversion": 0.7,
            "optimizer_turnover_penalty": 0.3,
        },
    },
]


class StrategyGalleryBuilder:
    def __init__(self, config: Config, prepared_data: PreparedData) -> None:
        self.config = config
        self.prepared_data = prepared_data
        self.output_dir = config.resolve_path(config.paths.get("output_dir", "output"))
        self.demo_dir = config.resolve_path(config.paths.get("demo_site_dir", "docs/demo"))

    def publish(self) -> tuple[list[Path], dict[str, object]]:
        ensure_directory(self.demo_dir)
        outputs: list[Path] = []

        base_strategy = MultiSignalStrategy(self.config.strategy)
        Backtester(
            self.prepared_data,
            base_strategy,
            output_dir=self.output_dir,
            transaction_cost_bps=float(self.config.strategy.get("transaction_cost_bps", 10.0)),
            commission_cost_bps=float(self.config.strategy.get("commission_cost_bps", 0.0)),
            slippage_cost_bps=float(
                self.config.strategy.get(
                    "slippage_cost_bps",
                    max(
                        float(self.config.strategy.get("transaction_cost_bps", 10.0))
                        - float(self.config.strategy.get("commission_cost_bps", 0.0)),
                        0.0,
                    ),
                )
            ),
        ).run()
        PerformanceReporter(self.config, self.prepared_data, self.output_dir).run()
        root_outputs, _ = DemoPublisher(self.config).publish()
        outputs.extend(root_outputs)

        gallery_dir = self.demo_dir / "gallery"
        ensure_directory(gallery_dir)
        cards = []
        for preset in self._preset_specs():
            card, preset_outputs = self._run_preset(preset, gallery_dir)
            outputs.extend(preset_outputs)
            cards.append(card)

        self._assign_tags(cards)
        outputs.extend(self._write_share_cards(cards))
        outputs.extend(self._write_winner_artifacts(cards))
        summary = {
            "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "gallery_dir": str(gallery_dir),
            "preset_count": len(cards),
            "available_tags": sorted({tag for card in cards for tag in card.get("tags", [])}),
            "presets": cards,
        }
        summary_path = self.demo_dir / "gallery_summary.json"
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        outputs.append(summary_path)

        gallery_path = self.demo_dir / "gallery.html"
        gallery_path.write_text(self._build_gallery_html(cards), encoding="utf-8")
        outputs.append(gallery_path)
        refreshed_outputs, _ = DemoPublisher(self.config).publish()
        outputs.extend(refreshed_outputs)
        return outputs, summary

    def _run_preset(self, preset: dict[str, object], gallery_dir: Path) -> tuple[dict[str, object], list[Path]]:
        slug = str(preset["slug"])
        title = str(preset["title"])
        description = str(preset["description"])
        overrides = dict(preset.get("overrides", {}))

        preset_output_dir = self.output_dir / "gallery" / slug
        preset_demo_dir = gallery_dir / slug
        preset_config = self._preset_config(
            output_dir=preset_output_dir,
            demo_dir=preset_demo_dir,
            strategy_overrides=overrides,
        )
        strategy = MultiSignalStrategy(preset_config.strategy)
        backtest_summary = Backtester(
            self.prepared_data,
            strategy,
            output_dir=preset_output_dir,
            transaction_cost_bps=float(preset_config.strategy.get("transaction_cost_bps", 10.0)),
            commission_cost_bps=float(preset_config.strategy.get("commission_cost_bps", 0.0)),
            slippage_cost_bps=float(
                preset_config.strategy.get(
                    "slippage_cost_bps",
                    max(
                        float(preset_config.strategy.get("transaction_cost_bps", 10.0))
                        - float(preset_config.strategy.get("commission_cost_bps", 0.0)),
                        0.0,
                    ),
                )
            ),
        ).run()
        _, report_summary = PerformanceReporter(preset_config, self.prepared_data, preset_output_dir).run()
        preset_outputs, bundle_summary = DemoPublisher(preset_config).publish()
        overrides_path = preset_demo_dir / "preset_overrides.json"
        overrides_path.write_text(json.dumps(overrides, indent=2), encoding="utf-8")
        preset_outputs.append(overrides_path)

        card = {
            "slug": slug,
            "title": title,
            "description": description,
            "report_path": f"gallery/{slug}/index.html",
            "dashboard_path": f"gallery/{slug}/report_dashboard.html",
            "override_path": f"gallery/{slug}/preset_overrides.json",
            "net_total_return": float(backtest_summary.get("total_return", 0.0)),
            "gross_total_return": float(backtest_summary.get("gross_total_return", 0.0)),
            "sharpe": float(backtest_summary.get("sharpe", 0.0)),
            "information_ratio": float(backtest_summary.get("information_ratio", 0.0)),
            "max_drawdown": float(backtest_summary.get("max_drawdown", 0.0)),
            "average_turnover": float(backtest_summary.get("average_turnover", 0.0)),
            "total_transaction_cost": float(backtest_summary.get("total_transaction_cost", 0.0)),
            "total_short_borrow_cost": float(backtest_summary.get("total_short_borrow_cost", 0.0)),
            "largest_aum_without_breach": report_summary.get("largest_aum_without_breach"),
            "first_breached_aum": report_summary.get("first_breached_aum"),
            "top_factor": (report_summary.get("top_factors_by_ic") or [None])[0],
            "best_month": report_summary.get("best_month"),
            "tags": [],
            "command_hint": f"python run_quant.py gallery --config {self.config.path.name}",
            "overrides": overrides,
            "bundle_summary": {
                "headline_metrics": bundle_summary.get("headline_metrics", {}),
                "top_security": bundle_summary.get("top_security"),
                "top_sector": bundle_summary.get("top_sector"),
            },
        }
        return card, preset_outputs

    def _preset_specs(self) -> list[dict[str, object]]:
        configured = self.config.strategy.get("gallery_presets")
        if isinstance(configured, list) and configured:
            presets = []
            for index, item in enumerate(configured):
                if not isinstance(item, dict):
                    continue
                slug = str(item.get("slug", f"preset_{index + 1}")).strip() or f"preset_{index + 1}"
                presets.append(
                    {
                        "slug": slug,
                        "title": str(item.get("title", slug.replace("_", " ").title())),
                        "description": str(item.get("description", "")),
                        "overrides": dict(item.get("overrides", {})),
                    }
                )
            if presets:
                return presets
        return copy.deepcopy(DEFAULT_GALLERY_PRESETS)

    def _preset_config(self, output_dir: Path, demo_dir: Path, strategy_overrides: dict[str, object]) -> Config:
        raw = copy.deepcopy(self.config.raw)
        raw.setdefault("paths", {})
        raw.setdefault("strategy", {})
        raw["paths"]["output_dir"] = str(output_dir)
        raw["paths"]["demo_site_dir"] = str(demo_dir)
        raw["strategy"].update(strategy_overrides)
        return Config(path=self.config.path, raw=raw)

    def _assign_tags(self, cards: list[dict[str, object]]) -> None:
        if not cards:
            return
        for tag, field, reverse in [
            ("best_return", "net_total_return", True),
            ("best_sharpe", "sharpe", True),
            ("highest_capacity", "largest_aum_without_breach", True),
            ("lowest_turnover", "average_turnover", False),
        ]:
            ranked = sorted(cards, key=lambda card: float(card.get(field) or 0.0), reverse=reverse)
            if ranked:
                ranked[0]["tags"].append(tag)

    def _write_share_cards(self, cards: list[dict[str, object]]) -> list[Path]:
        outputs: list[Path] = []
        for card in cards:
            preset_dir = self.demo_dir / "gallery" / str(card["slug"])
            ensure_directory(preset_dir)
            svg_path = preset_dir / "share_card.svg"
            html_path = preset_dir / "share_card.html"
            svg_path.write_text(self._build_share_card_svg(card), encoding="utf-8")
            html_path.write_text(self._build_share_card_html(card), encoding="utf-8")
            card["share_card_path"] = f"gallery/{card['slug']}/share_card.svg"
            card["share_card_html_path"] = f"gallery/{card['slug']}/share_card.html"
            outputs.extend([svg_path, html_path])
        return outputs

    def _write_winner_artifacts(self, cards: list[dict[str, object]]) -> list[Path]:
        winner = self._spotlight_card(cards)
        if winner is None:
            return []
        winner_payload = {
            "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "title": winner.get("title"),
            "slug": winner.get("slug"),
            "tags": list(winner.get("tags", [])),
            "report_path": winner.get("report_path"),
            "dashboard_path": winner.get("dashboard_path"),
            "share_card_path": winner.get("share_card_path"),
            "share_card_html_path": winner.get("share_card_html_path"),
            "net_total_return": winner.get("net_total_return"),
            "sharpe": winner.get("sharpe"),
            "max_drawdown": winner.get("max_drawdown"),
            "largest_aum_without_breach": winner.get("largest_aum_without_breach"),
            "average_turnover": winner.get("average_turnover"),
            "top_factor": winner.get("top_factor"),
            "best_month": winner.get("best_month"),
        }
        json_path = self.demo_dir / "latest_winner.json"
        json_path.write_text(json.dumps(winner_payload, indent=2), encoding="utf-8")
        badge_path = self.demo_dir / "latest_winner_badge.svg"
        badge_path.write_text(self._build_winner_badge_svg(winner_payload), encoding="utf-8")
        md_path = self.demo_dir / "latest_winner.md"
        md_path.write_text(self._build_winner_markdown(winner_payload), encoding="utf-8")
        readme_path = self.demo_dir / "latest_winner_readme_snippet.md"
        readme_path.write_text(self._build_winner_readme_snippet(winner_payload), encoding="utf-8")
        release_note_path = self.demo_dir / "latest_winner_release_note.md"
        release_note_path.write_text(self._build_winner_release_note(winner_payload), encoding="utf-8")
        social_post_path = self.demo_dir / "latest_winner_social_post.txt"
        social_post_path.write_text(self._build_winner_social_post(winner_payload), encoding="utf-8")
        return [json_path, badge_path, md_path, readme_path, release_note_path, social_post_path]

    def _build_share_card_svg(self, card: dict[str, object]) -> str:
        title = html.escape(str(card.get("title", "")))
        description = html.escape(str(card.get("description", "")))
        top_factor = html.escape(str((card.get("top_factor") or {}).get("factor", "-")))
        best_month = html.escape(str((card.get("best_month") or {}).get("month", "-")))
        tags = ", ".join(str(tag) for tag in card.get("tags", [])) or "preset"
        return f"""<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630" role="img" aria-labelledby="title desc">
  <title id="title">{title}</title>
  <desc id="desc">{description}</desc>
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#f7efe5"/>
      <stop offset="50%" stop-color="#f1e7db"/>
      <stop offset="100%" stop-color="#e9dfd2"/>
    </linearGradient>
    <radialGradient id="glowA" cx="0" cy="0" r="1">
      <stop offset="0%" stop-color="#c65c21" stop-opacity="0.28"/>
      <stop offset="100%" stop-color="#c65c21" stop-opacity="0"/>
    </radialGradient>
    <radialGradient id="glowB" cx="0" cy="0" r="1">
      <stop offset="0%" stop-color="#2a6171" stop-opacity="0.22"/>
      <stop offset="100%" stop-color="#2a6171" stop-opacity="0"/>
    </radialGradient>
  </defs>
  <rect width="1200" height="630" rx="32" fill="url(#bg)"/>
  <circle cx="120" cy="90" r="210" fill="url(#glowA)"/>
  <circle cx="1040" cy="120" r="240" fill="url(#glowB)"/>
  <rect x="36" y="36" width="1128" height="558" rx="28" fill="rgba(255,251,244,0.76)" stroke="rgba(74,56,37,0.10)"/>
  <text x="74" y="92" font-size="18" letter-spacing="4" fill="#b24f1d" font-family="Georgia, serif">QUANT STRATEGY SNAPSHOT</text>
  <text x="74" y="160" font-size="54" fill="#1c1714" font-family="Georgia, serif">{title}</text>
  <text x="74" y="204" font-size="22" fill="#6a5f55" font-family="Georgia, serif">{description}</text>
  <text x="74" y="246" font-size="16" fill="#6a5f55" font-family="Georgia, serif">Tags: {html.escape(tags)}</text>

  <rect x="74" y="290" width="230" height="118" rx="20" fill="rgba(255,255,255,0.75)" stroke="rgba(74,56,37,0.08)"/>
  <rect x="320" y="290" width="230" height="118" rx="20" fill="rgba(255,255,255,0.75)" stroke="rgba(74,56,37,0.08)"/>
  <rect x="566" y="290" width="230" height="118" rx="20" fill="rgba(255,255,255,0.75)" stroke="rgba(74,56,37,0.08)"/>
  <rect x="812" y="290" width="314" height="118" rx="20" fill="rgba(255,255,255,0.75)" stroke="rgba(74,56,37,0.08)"/>

  <text x="98" y="328" font-size="15" fill="#6a5f55" font-family="Arial, sans-serif">Net Return</text>
  <text x="98" y="384" font-size="38" fill="#171311" font-family="Georgia, serif">{self._format_pct(card.get("net_total_return", 0.0))}</text>

  <text x="344" y="328" font-size="15" fill="#6a5f55" font-family="Arial, sans-serif">Sharpe</text>
  <text x="344" y="384" font-size="38" fill="#171311" font-family="Georgia, serif">{self._format_number(card.get("sharpe", 0.0))}</text>

  <text x="590" y="328" font-size="15" fill="#6a5f55" font-family="Arial, sans-serif">Capacity</text>
  <text x="590" y="384" font-size="38" fill="#171311" font-family="Georgia, serif">{self._format_currency(card.get("largest_aum_without_breach"))}</text>

  <text x="836" y="328" font-size="15" fill="#6a5f55" font-family="Arial, sans-serif">Drawdown / Turnover</text>
  <text x="836" y="368" font-size="30" fill="#171311" font-family="Georgia, serif">{self._format_pct(card.get("max_drawdown", 0.0))} / {self._format_pct(card.get("average_turnover", 0.0))}</text>

  <rect x="74" y="438" width="512" height="122" rx="22" fill="rgba(178,79,29,0.08)" stroke="rgba(178,79,29,0.10)"/>
  <rect x="606" y="438" width="520" height="122" rx="22" fill="rgba(33,88,106,0.08)" stroke="rgba(33,88,106,0.10)"/>
  <text x="98" y="478" font-size="15" fill="#6a5f55" font-family="Arial, sans-serif">Top Factor</text>
  <text x="98" y="524" font-size="32" fill="#171311" font-family="Georgia, serif">{top_factor}</text>
  <text x="630" y="478" font-size="15" fill="#6a5f55" font-family="Arial, sans-serif">Best Month</text>
  <text x="630" y="524" font-size="32" fill="#171311" font-family="Georgia, serif">{best_month}</text>
  <text x="74" y="590" font-size="15" fill="#6a5f55" font-family="Arial, sans-serif">Generated by Quant Research Stack gallery export</text>
</svg>
"""

    def _build_share_card_html(self, card: dict[str, object]) -> str:
        title = html.escape(str(card.get("title", "")))
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title} Share Card</title>
  <style>
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background: #f4efe7;
      color: #191512;
      display: grid;
      place-items: center;
      min-height: 100vh;
      padding: 24px;
    }}
    .shell {{
      width: min(1100px, 100%);
      background: rgba(255,251,244,0.86);
      border: 1px solid rgba(71,52,31,0.10);
      border-radius: 24px;
      padding: 20px;
    }}
    img {{
      width: 100%;
      border-radius: 18px;
      border: 1px solid rgba(71,52,31,0.10);
      background: white;
    }}
    .links {{
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 14px;
    }}
    a {{
      color: #21586a;
      text-decoration: none;
    }}
  </style>
</head>
<body>
  <main class="shell">
    <img src="share_card.svg" alt="{title} share card">
    <div class="links">
      <a href="share_card.svg">Open SVG</a>
      <a href="index.html">Open preset bundle</a>
      <a href="report_dashboard.html">Open dashboard</a>
    </div>
  </main>
</body>
</html>
"""

    def _build_gallery_html(self, cards: list[dict[str, object]]) -> str:
        payload = json.dumps(cards, ensure_ascii=True)
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Quant Strategy Gallery</title>
  <style>
    :root {{
      --bg: #f3eee6;
      --panel: rgba(255, 251, 243, 0.92);
      --panel-strong: #fffaf3;
      --ink: #191512;
      --muted: #665d54;
      --line: rgba(71, 52, 31, 0.12);
      --accent: #b24f1d;
      --cool: #21586a;
      --shadow: 0 22px 60px rgba(54, 33, 13, 0.14);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(178, 79, 29, 0.16), transparent 28%),
        radial-gradient(circle at top right, rgba(33, 88, 106, 0.14), transparent 24%),
        linear-gradient(180deg, #f8f3eb 0%, var(--bg) 100%);
    }}
    .shell {{
      width: min(1240px, calc(100% - 32px));
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
      font-size: clamp(34px, 6vw, 66px);
      line-height: 0.94;
      max-width: 860px;
    }}
    .lede {{
      margin: 14px 0 0;
      max-width: 860px;
      color: var(--muted);
      font-size: 18px;
      line-height: 1.65;
    }}
    .cta-row {{
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 20px;
    }}
    .cta-row a {{
      display: inline-block;
      padding: 10px 14px;
      border-radius: 999px;
      text-decoration: none;
      background: var(--accent);
      color: white;
    }}
    .cta-row a.secondary {{
      background: rgba(33, 88, 106, 0.10);
      color: var(--cool);
    }}
    .section {{ margin-top: 20px; padding: 22px; }}
    .controls {{
      display: grid;
      grid-template-columns: 1.1fr 0.8fr 1fr;
      gap: 12px;
      margin-bottom: 18px;
    }}
    .control {{
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 12px 14px;
      background: var(--panel-strong);
    }}
    .control label {{
      display: block;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      margin-bottom: 8px;
    }}
    .control input, .control select {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 10px 12px;
      font: inherit;
      background: rgba(255,255,255,0.78);
      color: var(--ink);
    }}
    .card-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 16px;
    }}
    .card {{
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 18px;
      background: var(--panel-strong);
    }}
    .card h2 {{
      margin: 0;
      font-size: 24px;
    }}
    .desc {{
      color: var(--muted);
      font-size: 15px;
      line-height: 1.6;
      min-height: 48px;
    }}
    .metric-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-top: 14px;
    }}
    .metric {{
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 12px;
      background: rgba(255,255,255,0.72);
    }}
    .metric span {{
      display: block;
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .metric strong {{
      display: block;
      margin-top: 8px;
      font-size: 22px;
      font-weight: normal;
    }}
    .tag-row {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 10px;
    }}
    .tag {{
      display: inline-block;
      padding: 4px 8px;
      border-radius: 999px;
      font-size: 12px;
      background: rgba(178, 79, 29, 0.10);
      color: var(--accent);
    }}
    .links {{
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 14px;
    }}
    .links a {{
      color: var(--cool);
      text-decoration: none;
    }}
    .share-preview {{
      margin-top: 14px;
      border: 1px solid var(--line);
      border-radius: 18px;
      overflow: hidden;
      background: rgba(255,255,255,0.78);
    }}
    .share-preview img {{
      display: block;
      width: 100%;
      aspect-ratio: 1200 / 630;
      object-fit: cover;
    }}
    .mini {{
      margin-top: 14px;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.6;
    }}
    details {{
      margin-top: 14px;
      border-top: 1px solid var(--line);
      padding-top: 12px;
    }}
    details summary {{
      cursor: pointer;
      color: var(--cool);
    }}
    pre {{
      margin: 10px 0 0;
      padding: 12px;
      border-radius: 16px;
      background: #1f2428;
      color: #f3efe8;
      overflow: auto;
      font-size: 12px;
      line-height: 1.5;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: var(--panel-strong);
      border-radius: 20px;
      overflow: hidden;
    }}
    th, td {{
      padding: 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      font-size: 14px;
    }}
    th {{
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 12px;
    }}
    tr:last-child td {{ border-bottom: none; }}
    @media (max-width: 1024px) {{
      .controls {{ grid-template-columns: 1fr; }}
      .card-grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <div class="eyebrow">Strategy Gallery</div>
      <h1>Compare multiple presets instead of showing one lonely backtest.</h1>
      <p class="lede">This page turns the research stack into something people can browse. Each preset has its own bundle and dashboard, while this gallery gives a fast comparison of return, risk, capacity, and turnover tradeoffs.</p>
      <div class="cta-row">
        <a href="index.html">Open base demo</a>
        <a class="secondary" href="../index.html">Project landing</a>
      </div>
    </section>
    <section class="section">
      <div class="controls">
        <div class="control">
          <label for="search-input">Search preset</label>
          <input id="search-input" type="text" placeholder="quality, long-short, defensive">
        </div>
        <div class="control">
          <label for="sort-select">Sort by</label>
          <select id="sort-select">
            <option value="sharpe">Sharpe</option>
            <option value="net_total_return">Net return</option>
            <option value="largest_aum_without_breach">Capacity</option>
            <option value="max_drawdown">Drawdown</option>
            <option value="average_turnover">Turnover</option>
          </select>
        </div>
        <div class="control">
          <label for="tag-select">Filter tag</label>
          <select id="tag-select">
            <option value="">All presets</option>
          </select>
        </div>
      </div>
      <div class="card-grid" id="card-grid"></div>
    </section>
    <section class="section">
      <h2>Preset leaderboard</h2>
      <table>
        <thead>
          <tr>
            <th>Preset</th>
            <th>Net Return</th>
            <th>Sharpe</th>
            <th>Drawdown</th>
            <th>Capacity</th>
            <th>Turnover</th>
            <th>Tags</th>
          </tr>
        </thead>
        <tbody id="leaderboard-body"></tbody>
      </table>
    </section>
  </main>
  <script id="gallery-data" type="application/json">{payload}</script>
  <script>
    const presets = JSON.parse(document.getElementById("gallery-data").textContent);
    const cardGrid = document.getElementById("card-grid");
    const leaderboardBody = document.getElementById("leaderboard-body");
    const searchInput = document.getElementById("search-input");
    const sortSelect = document.getElementById("sort-select");
    const tagSelect = document.getElementById("tag-select");

    const uniqueTags = [...new Set(presets.flatMap((preset) => preset.tags || []))].sort();
    uniqueTags.forEach((tag) => {{
      const option = document.createElement("option");
      option.value = tag;
      option.textContent = tag;
      tagSelect.appendChild(option);
    }});

    const fmtPct = (value) => `${{(Number(value || 0) * 100).toFixed(2)}}%`;
    const fmtNum = (value) => Number(value || 0).toFixed(2);
    const fmtCurrency = (value) => value === null || value === undefined || value === "" ? "-" : `$${{Number(value).toLocaleString(undefined, {{ maximumFractionDigits: 0 }})}}`;
    const esc = (value) => String(value ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");

    function sortPresets(rows, field) {{
      const sorted = [...rows];
      sorted.sort((left, right) => {{
        const leftValue = Number(left[field] ?? 0);
        const rightValue = Number(right[field] ?? 0);
        if (field === "average_turnover") {{
          return leftValue - rightValue || Number(right.sharpe || 0) - Number(left.sharpe || 0);
        }}
        return rightValue - leftValue || Number(right.net_total_return || 0) - Number(left.net_total_return || 0);
      }});
      return sorted;
    }}

    function render() {{
      const query = searchInput.value.trim().toLowerCase();
      const tag = tagSelect.value;
      const sortField = sortSelect.value;
      const filtered = sortPresets(
        presets.filter((preset) => {{
          const haystack = [
            preset.title,
            preset.description,
            ...(preset.tags || []),
            ...Object.keys(preset.overrides || {{}}),
          ].join(" ").toLowerCase();
          if (query && !haystack.includes(query)) {{
            return false;
          }}
          if (tag && !(preset.tags || []).includes(tag)) {{
            return false;
          }}
          return true;
        }}),
        sortField,
      );

      cardGrid.innerHTML = filtered.map((preset) => `
        <article class="card">
          <div class="eyebrow">${{esc((preset.tags || []).join(", ") || "preset")}}</div>
          <h2>${{esc(preset.title)}}</h2>
          <p class="desc">${{esc(preset.description)}}</p>
          <div class="tag-row">${{(preset.tags || []).map((tagName) => `<span class="tag">${{esc(tagName)}}</span>`).join("")}}</div>
          <div class="metric-grid">
            <div class="metric"><span>Net Return</span><strong>${{fmtPct(preset.net_total_return)}}</strong></div>
            <div class="metric"><span>Sharpe</span><strong>${{fmtNum(preset.sharpe)}}</strong></div>
            <div class="metric"><span>Drawdown</span><strong>${{fmtPct(preset.max_drawdown)}}</strong></div>
            <div class="metric"><span>Capacity</span><strong>${{fmtCurrency(preset.largest_aum_without_breach)}}</strong></div>
            <div class="metric"><span>Turnover</span><strong>${{fmtPct(preset.average_turnover)}}</strong></div>
            <div class="metric"><span>Borrow Cost</span><strong>${{fmtPct(preset.total_short_borrow_cost)}}</strong></div>
          </div>
          <div class="links">
            <a href="${{esc(preset.report_path)}}">Open bundle</a>
            <a href="${{esc(preset.dashboard_path)}}">Open dashboard</a>
            <a href="${{esc(preset.share_card_html_path)}}">Share preview</a>
            <a href="${{esc(preset.share_card_path)}}">Share SVG</a>
            <a href="${{esc(preset.override_path)}}">Preset diff JSON</a>
          </div>
          <div class="share-preview">
            <img src="${{esc(preset.share_card_path)}}" alt="${{esc(preset.title)}} share card">
          </div>
          <div class="mini">
            <div>Top factor: ${{esc((preset.top_factor || {{}}).factor || "-")}}</div>
            <div>Best month: ${{esc((preset.best_month || {{}}).month || "-")}}</div>
          </div>
          <details>
            <summary>Show override diff</summary>
            <pre>${{esc(JSON.stringify(preset.overrides || {{}}, null, 2))}}</pre>
          </details>
        </article>
      `).join("");

      leaderboardBody.innerHTML = filtered.map((preset) => `
        <tr>
          <td>${{esc(preset.title)}}</td>
          <td>${{fmtPct(preset.net_total_return)}}</td>
          <td>${{fmtNum(preset.sharpe)}}</td>
          <td>${{fmtPct(preset.max_drawdown)}}</td>
          <td>${{fmtCurrency(preset.largest_aum_without_breach)}}</td>
          <td>${{fmtPct(preset.average_turnover)}}</td>
          <td>${{esc((preset.tags || []).join(", "))}}</td>
        </tr>
      `).join("");
    }}

    searchInput.addEventListener("input", render);
    sortSelect.addEventListener("change", render);
    tagSelect.addEventListener("change", render);
    render();
  </script>
</body>
</html>
"""

    def _format_pct(self, value: object) -> str:
        return f"{float(value or 0.0) * 100.0:.2f}%"

    def _format_currency(self, value: object) -> str:
        if value in (None, ""):
            return "-"
        return f"${float(value):,.0f}"

    def _format_number(self, value: object) -> str:
        return f"{float(value or 0.0):.2f}"

    def _spotlight_card(self, cards: list[dict[str, object]]) -> dict[str, object] | None:
        if not cards:
            return None
        for preferred_tag in ("best_return", "best_sharpe", "highest_capacity", "lowest_turnover"):
            for card in cards:
                if preferred_tag in list(card.get("tags", [])):
                    return card
        return cards[0]

    def _build_winner_badge_svg(self, winner: dict[str, object]) -> str:
        title = html.escape(str(winner.get("title", "-")))
        net_return = html.escape(self._format_pct(winner.get("net_total_return", 0.0)))
        sharpe = html.escape(self._format_number(winner.get("sharpe", 0.0)))
        capacity = html.escape(self._format_currency(winner.get("largest_aum_without_breach")))
        return f"""<svg xmlns="http://www.w3.org/2000/svg" width="620" height="160" viewBox="0 0 620 160" role="img" aria-labelledby="title desc">
  <title id="title">Latest Winner</title>
  <desc id="desc">{title} | return {net_return} | sharpe {sharpe}</desc>
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#f8efe4"/>
      <stop offset="100%" stop-color="#efe4d7"/>
    </linearGradient>
  </defs>
  <rect width="620" height="160" rx="24" fill="url(#bg)"/>
  <rect x="12" y="12" width="596" height="136" rx="18" fill="rgba(255,251,244,0.82)" stroke="rgba(71,52,31,0.10)"/>
  <text x="32" y="48" font-size="14" letter-spacing="3" fill="#b24f1d" font-family="Arial, sans-serif">LATEST WINNER</text>
  <text x="32" y="88" font-size="28" fill="#191512" font-family="Georgia, serif">{title}</text>
  <text x="32" y="126" font-size="18" fill="#655c53" font-family="Arial, sans-serif">Return {net_return} | Sharpe {sharpe} | Capacity {capacity}</text>
</svg>
"""

    def _build_winner_markdown(self, winner: dict[str, object]) -> str:
        title = str(winner.get("title", "-"))
        report_path = str(winner.get("report_path", ""))
        dashboard_path = str(winner.get("dashboard_path", ""))
        share_card_path = str(winner.get("share_card_path", ""))
        tags = ", ".join(str(tag) for tag in winner.get("tags", [])) or "preset"
        return "\n".join(
            [
                "# Latest Winner",
                "",
                f"- Preset: `{title}`",
                f"- Tags: `{tags}`",
                f"- Net return: `{self._format_pct(winner.get('net_total_return', 0.0))}`",
                f"- Sharpe: `{self._format_number(winner.get('sharpe', 0.0))}`",
                f"- Capacity: `{self._format_currency(winner.get('largest_aum_without_breach'))}`",
                f"- Bundle: `{report_path}`",
                f"- Dashboard: `{dashboard_path}`",
                f"- Share card: `{share_card_path}`",
                "",
            ]
        )

    def _build_winner_readme_snippet(self, winner: dict[str, object]) -> str:
        title = str(winner.get("title", "-"))
        report_path = str(winner.get("report_path", ""))
        dashboard_path = str(winner.get("dashboard_path", ""))
        share_card_path = str(winner.get("share_card_path", ""))
        return "\n".join(
            [
                f"![Latest Winner](latest_winner_badge.svg)",
                "",
                f"**Latest winner:** `{title}`",
                f"with `{self._format_pct(winner.get('net_total_return', 0.0))}` net return,",
                f"`{self._format_number(winner.get('sharpe', 0.0))}` Sharpe,",
                f"and `{self._format_currency(winner.get('largest_aum_without_breach'))}` capacity.",
                "",
                f"- Bundle: `{report_path}`",
                f"- Dashboard: `{dashboard_path}`",
                f"- Share card: `{share_card_path}`",
                "",
            ]
        )

    def _build_winner_release_note(self, winner: dict[str, object]) -> str:
        title = str(winner.get("title", "-"))
        top_factor = str((winner.get("top_factor") or {}).get("factor", "-"))
        best_month = str((winner.get("best_month") or {}).get("month", "-"))
        return "\n".join(
            [
                "## Latest Winner",
                "",
                f"- Preset: **{title}**",
                f"- Net return: **{self._format_pct(winner.get('net_total_return', 0.0))}**",
                f"- Sharpe: **{self._format_number(winner.get('sharpe', 0.0))}**",
                f"- Capacity: **{self._format_currency(winner.get('largest_aum_without_breach'))}**",
                f"- Turnover: **{self._format_pct(winner.get('average_turnover', 0.0))}**",
                f"- Top factor: **{top_factor}**",
                f"- Best month: **{best_month}**",
                "",
            ]
        )

    def _build_winner_social_post(self, winner: dict[str, object]) -> str:
        title = str(winner.get("title", "-"))
        tags = ", ".join(f"#{str(tag)}" for tag in winner.get("tags", [])) or "#quant"
        report_path = str(winner.get("report_path", ""))
        share_card_html_path = str(winner.get("share_card_html_path", ""))
        return (
            f"Latest winner: {title} | "
            f"Return {self._format_pct(winner.get('net_total_return', 0.0))} | "
            f"Sharpe {self._format_number(winner.get('sharpe', 0.0))} | "
            f"Capacity {self._format_currency(winner.get('largest_aum_without_breach'))}\n"
            f"Bundle: {report_path}\n"
            f"Share preview: {share_card_html_path}\n"
            f"{tags}"
        )
