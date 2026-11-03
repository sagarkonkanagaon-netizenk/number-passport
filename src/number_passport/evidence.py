"""The evidence pack: what an auditor receives, and how they re-check it.

A pack is a folder:

* ``passport.json``: the full machine-readable trace (hops, steps, quality, findings, audit log).
* ``passport.html``: the same, readable, with a lineage diagram. One file, no external assets.
* ``replay.sql``: every query that ran, in order, with its purpose and the result it returned.
* ``MANIFEST.sha256``: SHA-256 of the three files above, so any edit to the pack is detectable.

``verify_pack`` re-checks a pack: manifest integrity, whether the pipeline code changed since the pack
was generated, and (with a connection) re-runs every query and compares results.
"""

from __future__ import annotations

import hashlib
import html
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .connectors import Connector, ConnectorError
from .guard import GuardViolation, check_query, result_hash
from .trace import Passport

PACK_FILES = ("passport.json", "passport.html", "replay.sql")


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def write_pack(passport: Passport, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    data = passport.to_dict()
    (out_dir / "passport.json").write_text(
        json.dumps(data, indent=2, sort_keys=False) + "\n", encoding="utf-8"
    )
    (out_dir / "passport.html").write_text(render_html(data), encoding="utf-8")
    (out_dir / "replay.sql").write_text(render_replay(data), encoding="utf-8")
    manifest = "".join(f"{_sha(out_dir / f)}  {f}\n" for f in PACK_FILES)
    (out_dir / "MANIFEST.sha256").write_text(manifest, encoding="utf-8")
    return out_dir


def render_replay(data: dict[str, Any]) -> str:
    lines = [
        f"-- Number Passport replay script: {data['metric']['name']} ({data['project']})",
        f"-- generated {data['generated_at']} against {data['connection']} (dialect {data['dialect']})",
        "-- Every query below is read-only and returns totals or counts only.",
        "",
    ]
    for q in data["audit"]:
        lines.append(f"-- [{q['id']}] {q['purpose']}")
        lines.append(f"-- expected result: {json.dumps(q['result'])}  sha256 {q['sha256'][:16]}")
        lines.append(q["sql"].rstrip(";") + ";")
        lines.append("")
    return "\n".join(lines)


# --------------------------------------------------------------------------------------------- verify
@dataclass
class VerifyResult:
    ok: bool
    manifest_ok: bool
    code_changes: list[str] = field(default_factory=list)
    query_mismatches: list[str] = field(default_factory=list)
    queries_checked: int = 0
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


def verify_pack(pack: Path, project_root: Path | None, connector: Connector | None) -> VerifyResult:
    res = VerifyResult(ok=True, manifest_ok=True)
    manifest = pack / "MANIFEST.sha256"
    if not manifest.is_file():
        return VerifyResult(ok=False, manifest_ok=False, notes=["MANIFEST.sha256 missing"])
    for line in manifest.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        digest, name = line.split(None, 1)
        f = pack / name.strip()
        if not f.is_file() or _sha(f) != digest:
            res.manifest_ok = False
            res.notes.append(f"{name.strip()} was modified after the pack was generated")
    data = json.loads((pack / "passport.json").read_text(encoding="utf-8"))

    if project_root is not None:
        for rel, digest in data.get("code_files", {}).items():
            f = Path(rel) if Path(rel).is_absolute() else project_root / rel
            if not f.is_file():
                res.code_changes.append(f"{rel}: file no longer exists")
            elif _sha(f) != digest:
                res.code_changes.append(f"{rel}: changed since the passport was generated")
    else:
        res.notes.append("code hashes not checked (no project root given)")

    if connector is not None:
        if connector.dialect != data.get("dialect"):
            res.notes.append(
                f"connection dialect {connector.dialect} differs from the pack's {data.get('dialect')}"
            )
        for q in data.get("audit", []):
            try:
                check_query(q["sql"], connector.dialect, "allow_dimensions", set())
            except GuardViolation as e:
                if "safe dimension" not in str(e):
                    res.query_mismatches.append(f"[{q['id']}] refused by the guard: {e}")
                    continue
            try:
                rows = connector.query(q["sql"])
            except ConnectorError as e:
                res.query_mismatches.append(f"[{q['id']}] failed: {e}")
                continue
            res.queries_checked += 1
            if result_hash(rows) != q["sha256"]:
                res.query_mismatches.append(
                    f"[{q['id']}] {q['purpose']}: expected {q['result']}, got {[list(map(str, r)) for r in rows]}"
                )
    else:
        res.notes.append("queries not re-run (no connection)")
    res.ok = res.manifest_ok and not res.code_changes and not res.query_mismatches
    return res


# --------------------------------------------------------------------------------------------- HTML
def _money(v: str | None) -> str:
    if v is None:
        return "—"
    try:
        f = float(v)
    except ValueError:
        return html.escape(v)
    return f"{f:,.2f}"


def _signed(v: str | None) -> str:
    if v is None:
        return "—"
    f = float(v)
    if abs(f) < 0.005:
        return "0.00"
    return ("+" if f > 0 else "\u2212") + f"{abs(f):,.2f}"


_STATUS_TEXT = {
    "verified": ("Verified", "Every hop reconciles to its code and to the report."),
    "gaps_found": ("Gaps found", "At least one difference is not explained by any code. See findings."),
    "partially_verified": (
        "Partially verified",
        "Lineage is complete, but some code could not be re-derived.",
    ),
    "lineage_only": ("Lineage only", "No database connection: code lineage and steps, no values."),
}
_HOP_STATUS = {
    "reconciled": "ok",
    "source": "neutral",
    "gap": "bad",
    "error": "bad",
    "not_reproducible": "warn",
    "code_only": "neutral",
    "unknown": "neutral",
}


def _lineage_svg(data: dict[str, Any]) -> str:
    """Layered left-to-right DAG: report, then each table at its distance from the metric table."""
    edges = [tuple(e) for e in data["edges"]]
    start = data["metric"]["table"]
    depth: dict[str, int] = {start: 1}
    frontier = [start]
    while frontier:
        nxt = []
        for t in frontier:
            for up, down in edges:
                if down == t and up not in depth:
                    depth[up] = depth[t] + 1
                    nxt.append(up)
        frontier = nxt
    status = {h["target"]: h["status"] for h in data["hops"]}
    layers: dict[int, list[str]] = {}
    for t, d in depth.items():
        layers.setdefault(d, []).append(t)
    has_report = data.get("report_hop") is not None
    if has_report:
        layers[0] = ["REPORT"]
        status["REPORT"] = data["report_hop"]["status"]
    cols = sorted(layers)
    w_box, h_box, gap_x, gap_y = 190, 44, 70, 22
    max_rows = max(len(v) for v in layers.values())
    width = len(cols) * w_box + (len(cols) - 1) * gap_x + 20
    height = max_rows * h_box + (max_rows - 1) * gap_y + 20
    pos: dict[str, tuple[float, float]] = {}
    for ci, c in enumerate(cols):
        items = sorted(layers[c])
        total = len(items) * h_box + (len(items) - 1) * gap_y
        y0 = (height - total) / 2
        for ri, t in enumerate(items):
            pos[t] = (10 + ci * (w_box + gap_x), y0 + ri * (h_box + gap_y))
    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="Lineage from report to source tables">'
    ]
    parts.append(
        '<defs><marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
        'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" class="edge-head"/></marker></defs>'
    )
    link = list(edges)
    if has_report:
        link.append((start, "REPORT"))
    for up, down in link:
        if up not in pos or down not in pos:
            continue
        x1, y1 = pos[down][0] + w_box, pos[down][1] + h_box / 2
        x2, y2 = pos[up][0], pos[up][1] + h_box / 2
        mx = (x1 + x2) / 2
        parts.append(
            f'<path class="edge" d="M{x1},{y1} C{mx},{y1} {mx},{y2} {x2 - 2},{y2}" marker-end="url(#arr)"/>'
        )
    for t, (x, y) in pos.items():
        cls = _HOP_STATUS.get(status.get(t, "unknown"), "neutral")
        label = data["report_hop"]["target"].replace("report: ", "") if t == "REPORT" else t
        if len(label) > 24:
            label = label[:23] + "…"
        parts.append(
            f'<g class="node {cls}"><rect x="{x}" y="{y}" width="{w_box}" height="{h_box}" rx="6"/>'
            f'<text x="{x + 12}" y="{y + 27}">{html.escape(label)}</text></g>'
        )
    parts.append("</svg>")
    return "".join(parts)


def _steps_table(h: dict[str, Any]) -> str:
    if not h["steps"]:
        return '<p class="muted">No row-removing steps in this code.</p>'
    rows = []
    for s in h["steps"]:
        native = ""
        if s.get("native_value_removed"):
            native = (
                f'<div class="sub">dropped value in source units: {_money(s["native_value_removed"])} '
                f"({html.escape(s['native_measure'] or '')})</div>"
            )
        removed = "—" if s["rows_removed"] is None else f"{s['rows_removed']:,}"
        rows.append(
            f"<tr><td><span class='kind'>{html.escape(s['kind'].replace('_', ' '))}</span></td>"
            f"<td><code>{html.escape(s['description'])}</code>{native}</td>"
            f"<td class='num'>{removed}</td>"
            f"<td class='num'>{_signed(s['value_delta'])}</td>"
            f"<td><code class='loc'>{html.escape(s['where'])}</code></td></tr>"
        )
    return (
        "<div class='scroll'><table><thead><tr><th>Step</th><th>Code</th><th class='num'>Rows removed</th>"
        "<th class='num'>Effect on value</th><th>Where</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table></div>"
    )


def _quality(h: dict[str, Any]) -> str:
    out = []
    for q in h["quality"]:
        chips = "".join(
            f"<span class='chip {c['status']}'>{html.escape(str(c['check']))}: "
            f"{html.escape(str(c['value']))}</span>"
            for c in q["checks"]
        )
        out.append(f"<div class='chips'>{chips}</div>")
    return "".join(out)


def _hop_card(h: dict[str, Any], data: dict[str, Any]) -> str:
    st = h["status"]
    cls = _HOP_STATUS.get(st, "neutral")
    orch = "".join(
        f"<div class='sub'>runs as task <b>{html.escape(o['task_id'])}</b> in DAG "
        f"<b>{html.escape(o['dag_id'])}</b> · schedule <code>{html.escape(o['schedule'])}</code></div>"
        for o in h.get("orchestration") or []
    )
    if h["model_kind"] == "source":
        body = (
            f"<p class='muted'>Source table. No code in this project builds it, so the trace ends here."
            f"{' Mixed units possible (e.g. several currencies).' if h['role'] == 'primary' else ''}</p>"
        )
        figures = (
            f"<div class='fig'><span>{html.escape(h['measure'])}</span><b>{_money(h['target_value'])}</b></div>"
            if h["target_value"]
            else ""
        )
    else:
        figures = (
            (
                f"<div class='fig'><span>In the table</span><b>{_money(h['target_value'])}</b></div>"
                f"<div class='fig'><span>Code re-run now</span><b>{_money(h['recomputed_value'])}</b></div>"
                f"<div class='fig'><span>Before any filter</span><b>{_money(h['relaxed_value'])}</b></div>"
                f"<div class='fig {'bad' if st == 'gap' else ''}'><span>Unexplained</span>"
                f"<b>{_signed(h['residual'])}</b></div>"
            )
            if data["mode"] == "full"
            else ""
        )
        body = _steps_table(h)
    factors = ""
    if h.get("factors"):
        lead = "other columns used: " if h["model_kind"] == "source" else "also uses "
        factors = (
            f"<div class='sub'>{lead}"
            + ", ".join(f"<code>{html.escape(f)}</code>" for f in h["factors"])
            + "</div>"
        )
    findings = "".join(
        f"<li class='{f['severity']}'><b>{html.escape(f['code'].replace('_', ' '))}</b> "
        f"{html.escape(f['message'])}{' · <code>' + html.escape(f['where']) + '</code>' if f.get('where') else ''}</li>"
        for f in h["findings"]
    )
    return f"""
<section class="hop {cls}" id="hop-{h["index"]}">
  <header>
    <div class="hop-no">Hop {h["index"]}{" · factor" if h["role"] == "factor" else ""}</div>
    <h3><code>{html.escape(h["target"])}</code> <span class="measure">{html.escape(h["measure"])}</span></h3>
    <span class="pill {cls}">{html.escape(st.replace("_", " "))}</span>
  </header>
  <div class="sub">built by <code>{html.escape(h["produced_by"] or "—")}</code>
  {("· " + html.escape(h["model_kind"])) if h["model_kind"] not in (None, "source") else ""}</div>
  {orch}{factors}
  <div class="figs">{figures}</div>
  {body}
  {_quality(h)}
  {'<ul class="findings">' + findings + "</ul>" if findings else ""}
</section>"""


def render_html(data: dict[str, Any]) -> str:
    title, blurb = _STATUS_TEXT.get(data["status"], (data["status"], ""))
    m = data["metric"]
    rh = data.get("report_hop")
    headline_value = rh["target_value"] if rh else (data["hops"][0]["target_value"] if data["hops"] else None)
    report_block = ""
    if rh:
        cls = _HOP_STATUS.get(rh["status"], "neutral")
        report_block = f"""
<section class="hop {cls}" id="hop-0">
  <header><div class="hop-no">Hop 0 · the report</div>
  <h3>{html.escape(rh["target"].replace("report: ", ""))}</h3><span class="pill {cls}">{html.escape(rh["status"])}</span></header>
  <div class="sub">as printed in <code>{html.escape(rh["produced_by"] or "—")}</code></div>
  <div class="figs"><div class="fig"><span>On the report</span><b>{_money(rh["target_value"])}</b></div>
  <div class="fig"><span><code>{html.escape(m["table"])}</code> total</span><b>{_money(rh["recomputed_value"])}</b></div>
  <div class="fig {"bad" if rh["status"] == "gap" else ""}"><span>Unexplained</span><b>{_signed(rh["residual"])}</b></div></div>
  <ul class="findings">{"".join(f"<li class='{f['severity']}'><b>{html.escape(f['code'].replace('_', ' '))}</b> {html.escape(f['message'])}</li>" for f in rh["findings"])}</ul>
</section>"""
    findings_all = [f for h in ([rh] if rh else []) + data["hops"] for f in h["findings"]]
    crit = [f for f in findings_all if f["severity"] == "critical"]
    crit_html = "".join(f"<li>{html.escape(f['message'])}</li>" for f in crit) or "<li>None.</li>"
    audit_rows = "".join(
        f"<tr><td class='num'>{q['id']}</td><td>{html.escape(q['purpose'])}</td>"
        f"<td><code>{html.escape(json.dumps(q['result']))}</code></td><td class='num'>{q['ms']}</td></tr>"
        for q in data["audit"]
    )
    files_rows = "".join(
        f"<tr><td><code>{html.escape(k)}</code></td><td><code class='hash'>{v[:16]}…</code></td></tr>"
        for k, v in data["code_files"].items()
    )
    where = f" where <code>{html.escape(m['where'])}</code>" if m.get("where") else ""
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Number Passport · {html.escape(m["label"] or m["name"])}</title>
<style>
:root {{ --bg:#f6f7f4; --paper:#ffffff; --ink:#15202b; --muted:#5b6670; --line:#d9dee2; --navy:#183a5a;
  --ok:#1f7a4d; --ok-bg:#e3f2ea; --bad:#b3261e; --bad-bg:#fbe7e5; --warn:#8a5a00; --warn-bg:#fdf1d8;
  --neutral:#44515c; --neutral-bg:#eef1f3; --code-bg:#f0f2f4; }}
@media (prefers-color-scheme: dark) {{ :root {{ --bg:#0f1418; --paper:#161d23; --ink:#e7ecef; --muted:#9aa6b0;
  --line:#2a343c; --navy:#8fb7dd; --ok:#5fc795; --ok-bg:#16301f; --bad:#f08a80; --bad-bg:#3a1a17; --warn:#e6b45c;
  --warn-bg:#36290f; --neutral:#b7c2cb; --neutral-bg:#1f282f; --code-bg:#1d252c; color-scheme: dark; }} }}
* {{ box-sizing:border-box }}
body {{ margin:0; background:var(--bg); color:var(--ink); font:15px/1.55 "IBM Plex Sans", "Segoe UI", system-ui, sans-serif; }}
main {{ max-width:1080px; margin:0 auto; padding:32px max(16px, env(safe-area-inset-left)) 64px; }}
code {{ font-family:"IBM Plex Mono", ui-monospace, Menlo, Consolas, monospace; font-size:.86em; background:var(--code-bg); padding:1px 5px; border-radius:4px; }}
h1 {{ font-size:28px; line-height:1.2; margin:6px 0 4px; text-wrap:balance }}
h2 {{ font-size:13px; letter-spacing:.08em; text-transform:uppercase; color:var(--muted); margin:36px 0 12px }}
.eyebrow {{ color:var(--navy); font-weight:600; letter-spacing:.08em; text-transform:uppercase; font-size:12px }}
.cover {{ background:var(--paper); border:1px solid var(--line); border-radius:12px; padding:24px; display:grid; grid-template-columns:1fr auto; gap:20px; align-items:start }}
.big {{ font-variant-numeric:tabular-nums; font-size:34px; font-weight:600; margin-top:10px }}
.meta {{ color:var(--muted); font-size:13px; margin-top:8px }}
.stamp {{ border:3px solid currentColor; border-radius:10px; padding:10px 16px; text-align:center; transform:rotate(-4deg); min-width:150px }}
.stamp b {{ display:block; font-size:20px; letter-spacing:.06em; text-transform:uppercase }}
.stamp span {{ font-size:12px }}
.stamp.verified {{ color:var(--ok) }} .stamp.gaps_found {{ color:var(--bad) }} .stamp.partially_verified, .stamp.lineage_only {{ color:var(--warn) }}
.diagram {{ background:var(--paper); border:1px solid var(--line); border-radius:12px; padding:16px; overflow-x:auto }}
.diagram svg {{ min-width:600px; width:100%; height:auto }}
.node rect {{ fill:var(--neutral-bg); stroke:var(--neutral); stroke-width:1.2 }}
.node.ok rect {{ fill:var(--ok-bg); stroke:var(--ok) }} .node.bad rect {{ fill:var(--bad-bg); stroke:var(--bad) }} .node.warn rect {{ fill:var(--warn-bg); stroke:var(--warn) }}
.node text {{ fill:var(--ink); font:13px "IBM Plex Mono", ui-monospace, monospace }}
.edge {{ fill:none; stroke:var(--muted); stroke-width:1.3 }} .edge-head {{ fill:var(--muted) }}
.hop {{ background:var(--paper); border:1px solid var(--line); border-left:5px solid var(--neutral); border-radius:10px; padding:18px 20px; margin:14px 0 }}
.hop.ok {{ border-left-color:var(--ok) }} .hop.bad {{ border-left-color:var(--bad) }} .hop.warn {{ border-left-color:var(--warn) }}
.hop header {{ display:flex; flex-wrap:wrap; align-items:baseline; gap:10px }}
.hop h3 {{ margin:0; font-size:17px; flex:1 1 auto; min-width:0 }}
.hop-no {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.06em; width:100% }}
.measure {{ color:var(--muted); font-weight:400; font-size:14px }}
.pill {{ font-size:12px; padding:2px 10px; border-radius:999px; background:var(--neutral-bg); color:var(--neutral); text-transform:capitalize }}
.pill.ok {{ background:var(--ok-bg); color:var(--ok) }} .pill.bad {{ background:var(--bad-bg); color:var(--bad) }} .pill.warn {{ background:var(--warn-bg); color:var(--warn) }}
.sub {{ color:var(--muted); font-size:13px; margin-top:4px }}
.figs {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(170px, 1fr)); gap:10px; margin:14px 0 }}
.fig {{ border:1px solid var(--line); border-radius:8px; padding:10px 12px }}
.fig span {{ display:block; color:var(--muted); font-size:12px }}
.fig b {{ font-variant-numeric:tabular-nums; font-size:18px; font-weight:600 }}
.fig.bad {{ border-color:var(--bad); background:var(--bad-bg) }} .fig.bad b {{ color:var(--bad) }}
.scroll {{ overflow-x:auto }}
table {{ width:100%; border-collapse:collapse; font-size:13.5px }}
th, td {{ text-align:left; padding:8px 10px; border-bottom:1px solid var(--line); vertical-align:top }}
th {{ color:var(--muted); font-weight:500; font-size:12px }}
.num {{ text-align:right; font-variant-numeric:tabular-nums; white-space:nowrap }}
.kind {{ font-size:12px; text-transform:uppercase; letter-spacing:.04em; color:var(--navy); white-space:nowrap }}
.loc {{ white-space:nowrap }}
.chips {{ display:flex; flex-wrap:wrap; gap:6px; margin-top:12px }}
.chip {{ font-size:12px; padding:3px 9px; border-radius:6px; background:var(--neutral-bg) }}
.chip.pass {{ background:var(--ok-bg); color:var(--ok) }} .chip.warn {{ background:var(--warn-bg); color:var(--warn) }} .chip.fail {{ background:var(--bad-bg); color:var(--bad) }}
.findings {{ margin:12px 0 0; padding-left:18px }} .findings li {{ margin:4px 0 }}
.findings li.critical {{ color:var(--bad) }} .findings li.warning {{ color:var(--warn) }}
.muted {{ color:var(--muted) }}
.summary {{ background:var(--paper); border:1px solid var(--line); border-radius:12px; padding:16px 20px }}
.summary ul {{ margin:6px 0 0; padding-left:18px }}
details {{ background:var(--paper); border:1px solid var(--line); border-radius:10px; padding:12px 16px; margin:10px 0 }}
summary {{ cursor:pointer; font-weight:600 }}
.hash {{ color:var(--muted) }}
footer {{ color:var(--muted); font-size:12px; margin-top:40px }}
@media (max-width: 640px) {{ .cover {{ grid-template-columns:1fr }} .stamp {{ justify-self:start }} .big {{ font-size:26px }} }}
@media print {{ body {{ background:#fff }} .hop, .cover, .diagram {{ break-inside:avoid }} }}
</style></head>
<body><main>
<div class="cover">
  <div>
    <div class="eyebrow">Number Passport · {html.escape(data["project"])}</div>
    <h1>{html.escape(m["label"] or m["name"])}</h1>
    <div class="big">{_money(headline_value)}</div>
    <div class="meta">{html.escape(m["agg"].upper())}(<code>{html.escape(m["table"])}.{html.escape(m["column"])}</code>){where}
      · {data["summary"]["hops"]} hops · {data["summary"]["critical"]} critical · {data["summary"]["warnings"]} warnings</div>
    <div class="meta">Generated {html.escape(data["generated_at"])}{" · as of " + html.escape(str(data["as_of"])) if data.get("as_of") else ""}
      · {html.escape(str(data["connection"] or "no connection"))}</div>
  </div>
  <div class="stamp {html.escape(data["status"])}"><b>{html.escape(title)}</b><span>{html.escape(blurb)}</span></div>
</div>

<h2>Needs attention</h2>
<div class="summary"><ul>{crit_html}</ul></div>

<h2>Lineage</h2>
<div class="diagram">{_lineage_svg(data)}</div>

<h2>Hop by hop</h2>
{report_block}
{"".join(_hop_card(h, data) for h in data["hops"])}

<h2>Re-run it yourself</h2>
<details><summary>{len(data["audit"])} queries ran, all read-only, totals and counts only</summary>
<p class="muted">The same queries, with expected results, are in <code>replay.sql</code>. Run
<code>passport verify &lt;this folder&gt;</code> to re-execute them and compare.</p>
<div class="scroll"><table><thead><tr><th class="num">#</th><th>Purpose</th><th>Result</th><th class="num">ms</th></tr></thead>
<tbody>{audit_rows}</tbody></table></div></details>
<details><summary>Code fingerprints ({len(data["code_files"])} files)</summary>
<p class="muted">SHA-256 of every file the trace read. <code>passport verify</code> flags any file changed since.</p>
<div class="scroll"><table><tbody>{files_rows}</tbody></table></div></details>

<footer>number-passport {html.escape(data["tool"]["version"])} · passport format v{data["passport_version"]}
 · mode {html.escape(data["mode"])}{(" · parse issues: " + html.escape("; ".join(data["parse_issues"]))) if data["parse_issues"] else ""}</footer>
</main></body></html>
"""
