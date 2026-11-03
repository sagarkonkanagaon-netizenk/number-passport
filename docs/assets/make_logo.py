"""Generate the animated Trace N logo set.

    python docs/assets/make_logo.py

The N is the lineage path: a source node (bottom left), two hops, and a verified check (top right). A
pulse of light travels the path every cycle, each node lights up as the pulse reaches it, and the check
pops when the trace completes. The mark is drawn in layers (extrusion, body, bevel highlight and glow) so it
reads as a lit 3D object. Animations are pure CSS inside the SVG, so they play on GitHub, and they stop
for viewers who ask for reduced motion.
"""

from __future__ import annotations

from pathlib import Path

HERE = Path(__file__).resolve().parent
PATH = "M36 92 V36 L92 92 V36"  # length 191.2
CYCLE = "4.2s"

STYLE = f"""
  <style>
    .np-float {{ animation: np-float 6s ease-in-out infinite; transform-origin: 64px 64px; }}
    .np-pulse {{ stroke-dasharray: 26 400; stroke-dashoffset: 26; animation: np-run {CYCLE} cubic-bezier(.55,.05,.35,1) infinite; }}
    .np-glow  {{ animation: np-breathe {CYCLE} ease-in-out infinite; }}
    .np-n1 {{ animation: np-n1 {CYCLE} ease-out infinite; transform-box: fill-box; transform-origin: center; }}
    .np-n2 {{ animation: np-n2 {CYCLE} ease-out infinite; transform-box: fill-box; transform-origin: center; }}
    .np-n3 {{ animation: np-n3 {CYCLE} ease-out infinite; transform-box: fill-box; transform-origin: center; }}
    .np-seal {{ animation: np-seal {CYCLE} cubic-bezier(.3,1.6,.5,1) infinite; transform-box: fill-box; transform-origin: center; }}
    .np-ring {{ animation: np-ring {CYCLE} ease-out infinite; transform-box: fill-box; transform-origin: center; }}
    .np-sheen {{ animation: np-sheen 6s ease-in-out infinite; }}
    @keyframes np-float {{ 0%,100% {{ transform: translateY(0) }} 50% {{ transform: translateY(-2.2px) }} }}
    @keyframes np-run {{ 0% {{ stroke-dashoffset: 26 }} 62% {{ stroke-dashoffset: -191 }} 100% {{ stroke-dashoffset: -191 }} }}
    @keyframes np-breathe {{ 0%,100% {{ opacity: .45 }} 66% {{ opacity: .95 }} }}
    @keyframes np-n1 {{ 0%,4% {{ transform: scale(1.35); filter: brightness(1.7) }} 20%,100% {{ transform: scale(1); filter: none }} }}
    @keyframes np-n2 {{ 0%,16% {{ transform: scale(1) }} 22% {{ transform: scale(1.4); filter: brightness(1.7) }} 36%,100% {{ transform: scale(1); filter: none }} }}
    @keyframes np-n3 {{ 0%,40% {{ transform: scale(1) }} 46% {{ transform: scale(1.4); filter: brightness(1.7) }} 60%,100% {{ transform: scale(1); filter: none }} }}
    @keyframes np-seal {{ 0%,58% {{ transform: scale(1) }} 66% {{ transform: scale(1.28) }} 74%,100% {{ transform: scale(1) }} }}
    @keyframes np-ring {{ 0%,60% {{ transform: scale(1); opacity: 0 }} 64% {{ opacity: .9 }} 90%,100% {{ transform: scale(2.3); opacity: 0 }} }}
    @keyframes np-sheen {{ 0%,55% {{ transform: translateX(-170px) }} 85%,100% {{ transform: translateX(170px) }} }}
    @media (prefers-reduced-motion: reduce) {{
      .np-float, .np-pulse, .np-glow, .np-n1, .np-n2, .np-n3, .np-seal, .np-ring, .np-sheen {{ animation: none; }}
      .np-pulse {{ stroke-dashoffset: -191; }}
    }}
  </style>"""

DEFS = """
  <defs>
    <linearGradient id="np-tile" x1="0" y1="0" x2="0.35" y2="1">
      <stop offset="0" stop-color="#1B3554"/>
      <stop offset="0.55" stop-color="#0F1F33"/>
      <stop offset="1" stop-color="#08111D"/>
    </linearGradient>
    <linearGradient id="np-rim" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0" stop-color="#7FB2FF" stop-opacity=".55"/>
      <stop offset=".5" stop-color="#7FB2FF" stop-opacity=".08"/>
      <stop offset="1" stop-color="#3FD0A4" stop-opacity=".25"/>
    </linearGradient>
    <linearGradient id="np-body" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="#8CF7D5"/>
      <stop offset="0.5" stop-color="#3FD0A4"/>
      <stop offset="1" stop-color="#1FA884"/>
    </linearGradient>
    <radialGradient id="np-node" cx="0.35" cy="0.3" r="0.8">
      <stop offset="0" stop-color="#E4EEFF"/>
      <stop offset="0.45" stop-color="#8DB9FF"/>
      <stop offset="1" stop-color="#3E6FC2"/>
    </radialGradient>
    <radialGradient id="np-sealg" cx="0.35" cy="0.3" r="0.85">
      <stop offset="0" stop-color="#C8FFEE"/>
      <stop offset="0.5" stop-color="#3FD0A4"/>
      <stop offset="1" stop-color="#14876A"/>
    </radialGradient>
    <linearGradient id="np-sheen-g" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0" stop-color="#fff" stop-opacity="0"/>
      <stop offset="0.5" stop-color="#fff" stop-opacity=".16"/>
      <stop offset="1" stop-color="#fff" stop-opacity="0"/>
    </linearGradient>
    <filter id="np-blur" x="-50%" y="-50%" width="200%" height="200%"><feGaussianBlur stdDeviation="5"/></filter>
    <filter id="np-blur-s" x="-50%" y="-50%" width="200%" height="200%"><feGaussianBlur stdDeviation="2.2"/></filter>
    <filter id="np-shadow" x="-30%" y="-30%" width="160%" height="170%">
      <feDropShadow dx="0" dy="5" stdDeviation="5" flood-color="#000" flood-opacity=".45"/>
    </filter>
    <clipPath id="np-clip"><rect x="0" y="0" width="128" height="128" rx="28"/></clipPath>
  </defs>"""


def mark() -> str:
    return f"""
  <g class="np-float">
    <g filter="url(#np-shadow)">
      <rect x="0" y="0" width="128" height="128" rx="28" fill="url(#np-tile)"/>
    </g>
    <rect x="0.75" y="0.75" width="126.5" height="126.5" rx="27.3" fill="none" stroke="url(#np-rim)" stroke-width="1.5"/>
    <g clip-path="url(#np-clip)">
      <rect class="np-sheen" x="-30" y="-20" width="60" height="170" fill="url(#np-sheen-g)" transform="rotate(18)"/>
    </g>
    <!-- glow -->
    <path class="np-glow" d="{PATH}" fill="none" stroke="#3FD0A4" stroke-width="14" stroke-linecap="round" stroke-linejoin="round" filter="url(#np-blur)"/>
    <!-- extrusion (depth) -->
    <path d="{PATH}" transform="translate(2.2 3.2)" fill="none" stroke="#0B5C48" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>
    <path d="{PATH}" transform="translate(1.1 1.6)" fill="none" stroke="#138069" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>
    <!-- body -->
    <path d="{PATH}" fill="none" stroke="url(#np-body)" stroke-width="10" stroke-linecap="round" stroke-linejoin="round"/>
    <!-- bevel highlight -->
    <path d="{PATH}" transform="translate(-1.3 -1.6)" fill="none" stroke="#E9FFF8" stroke-opacity=".45" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
    <!-- travelling pulse -->
    <path class="np-pulse" d="{PATH}" fill="none" stroke="#B9FFE9" stroke-width="12" stroke-linecap="round" stroke-linejoin="round" filter="url(#np-blur-s)"/>
    <path class="np-pulse" d="{PATH}" fill="none" stroke="#FFFFFF" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>
    <!-- nodes -->
    <g class="np-n1"><circle cx="36" cy="92" r="9.5" fill="#0F1F33" stroke="url(#np-node)" stroke-width="5.5"/></g>
    <g class="np-n2"><circle cx="36" cy="36" r="7.5" fill="url(#np-node)"/><circle cx="33.6" cy="33.4" r="2.2" fill="#fff" opacity=".8"/></g>
    <g class="np-n3"><circle cx="92" cy="92" r="7.5" fill="url(#np-node)"/><circle cx="89.6" cy="89.4" r="2.2" fill="#fff" opacity=".8"/></g>
    <!-- verified seal -->
    <circle class="np-ring" cx="92" cy="36" r="12.5" fill="none" stroke="#3FD0A4" stroke-width="2.5"/>
    <g class="np-seal">
      <circle cx="93" cy="38" r="12.5" fill="#0B5C48"/>
      <circle cx="92" cy="36" r="12.5" fill="url(#np-sealg)"/>
      <path d="M86.3 36.4 L90.5 40.6 L98.2 32.4" fill="none" stroke="#0A1A2A" stroke-width="3.8" stroke-linecap="round" stroke-linejoin="round"/>
    </g>
  </g>"""


FONT = "Inter, -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif"


def svg(view: str, w: int, h: int, body: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{view}" width="{w}" height="{h}" role="img" '
        f'aria-labelledby="np-title">\n  <title id="np-title">Number Passport</title>{STYLE}{DEFS}{body}\n</svg>\n'
    )


def wordmark(name: str, title: str, tag: str) -> None:
    body = f"""
  <g transform="translate(8 8)">{mark()}
  </g>
  <text x="166" y="78" font-family="{FONT}" font-size="54" font-weight="800" letter-spacing="-1.4" fill="{title}">Number Passport</text>
  <text x="168" y="114" font-family="{FONT}" font-size="17.5" font-weight="600" letter-spacing="3.2" fill="{tag}">EVERY REPORTED NUMBER, PROVEN</text>"""
    (HERE / name).write_text(svg("0 0 680 150", 680, 150, body), encoding="utf-8")


def main() -> None:
    (HERE / "logo-mark.svg").write_text(svg("-8 -8 144 150", 144, 150, mark()), encoding="utf-8")
    wordmark("logo-light.svg", "#0E1B2C", "#1F9C7B")
    wordmark("logo-dark.svg", "#F0F3F6", "#3FD0A4")
    banner = f"""
  <defs>
    <radialGradient id="bg" cx="0.22" cy="0.5" r="0.7"><stop offset="0" stop-color="#17375A"/><stop offset="1" stop-color="#070F1A"/></radialGradient>
    <pattern id="dots" width="32" height="32" patternUnits="userSpaceOnUse"><circle cx="2" cy="2" r="1.4" fill="#7FB2FF" fill-opacity=".10"/></pattern>
  </defs>
  <rect width="1280" height="640" fill="url(#bg)"/>
  <rect width="1280" height="640" fill="url(#dots)"/>
  <circle cx="254" cy="320" r="190" fill="#3FD0A4" opacity=".07" filter="url(#np-blur)"/>
  <g transform="translate(110 170) scale(2.25)">{mark()}
  </g>
  <g font-family="{FONT}">
    <text x="470" y="262" font-size="78" font-weight="800" letter-spacing="-2.2" fill="#F0F3F6">Number Passport</text>
    <text x="474" y="320" font-size="30" font-weight="500" fill="#A9C4E8">Trace any reported number to its source rows.</text>
    <text x="474" y="362" font-size="30" font-weight="500" fill="#A9C4E8">Reconcile every hop. Prove it to the auditor.</text>
    <g font-size="21" font-weight="700">
      <rect x="474" y="410" width="132" height="44" rx="22" fill="#3FD0A4"/>
      <text x="540" y="439" text-anchor="middle" fill="#0E1B2C">BCBS 239</text>
      <rect x="620" y="410" width="230" height="44" rx="22" fill="none" stroke="#7FB2FF" stroke-opacity=".55" stroke-width="2"/>
      <text x="735" y="439" text-anchor="middle" fill="#F0F3F6">SQL · dbt · PySpark</text>
      <rect x="864" y="410" width="230" height="44" rx="22" fill="none" stroke="#7FB2FF" stroke-opacity=".55" stroke-width="2"/>
      <text x="979" y="439" text-anchor="middle" fill="#F0F3F6">CLI · MCP · Skill</text>
    </g>
    <text x="474" y="540" font-size="22" font-weight="500" fill="#5B7DA6">github.com/sagarkonkanagaon-netizenk/number-passport</text>
  </g>"""
    (HERE / "social-preview.svg").write_text(svg("0 0 1280 640", 1280, 640, banner), encoding="utf-8")
    readme_banner()


def readme_banner() -> None:
    """Wide animated banner for the top of the README: the mark, the name, and a live trace across the
    medallion layers that reconciles hop by hop."""
    chips = [
        ("bronze.loans", "source"),
        ("silver.exposures", "reconciled"),
        ("gold.rwa_by_class", "reconciled"),
        ("Q3 capital report", "verified"),
    ]
    x0, y, w, gap = 330, 318, 208, 26
    parts = []
    for i, (name, label) in enumerate(chips):
        x = x0 + i * (w + gap)
        delay = f"{0.35 + i * 0.75:.2f}s"
        parts.append(f"""
    <g class="bn-chip" style="animation-delay:{delay}">
      <rect x="{x}" y="{y}" width="{w}" height="64" rx="14" fill="#0F2136" stroke="#2A4B70" stroke-width="1.5"/>
      <text x="{x + 18}" y="{y + 28}" font-size="16" font-weight="700" fill="#E6EEF8">{name}</text>
      <text x="{x + 18}" y="{y + 50}" font-size="13" font-weight="600" letter-spacing="1.6" fill="#7FB2FF">{label.upper()}</text>
      <g class="bn-tick" style="animation-delay:{delay}">
        <circle cx="{x + w - 22}" cy="{y + 32}" r="11" fill="#3FD0A4"/>
        <path d="M{x + w - 27} {y + 32} l3.6 3.6 l6.6 -7" fill="none" stroke="#0A1A2A" stroke-width="2.8" stroke-linecap="round" stroke-linejoin="round"/>
      </g>
    </g>""")
        if i < len(chips) - 1:
            lx = x + w
            parts.append(f"""
    <path d="M{lx + 2} {y + 32} H{lx + gap - 2}" stroke="#2A4B70" stroke-width="2"/>
    <circle class="bn-dot" style="animation-delay:{delay}" cx="{lx + 2}" cy="{y + 32}" r="4" fill="#B9FFE9"/>""")
    style = """
  <style>
    .bn-chip { animation: bn-chip 4.2s ease-out infinite both; }
    .bn-tick { animation: bn-tick 4.2s cubic-bezier(.3,1.6,.5,1) infinite both; transform-box: fill-box; transform-origin: center; }
    .bn-dot { animation: bn-dot 4.2s ease-in-out infinite both; }
    .bn-scan { animation: bn-scan 8s linear infinite; }
    @keyframes bn-chip { 0% { opacity: .35 } 8%,92% { opacity: 1 } 100% { opacity: .35 } }
    @keyframes bn-tick { 0%,6% { transform: scale(0) } 14%,90% { transform: scale(1) } 100% { transform: scale(0) } }
    @keyframes bn-dot { 0% { transform: translateX(0); opacity: 0 } 4% { opacity: 1 } 16% { transform: translateX(22px); opacity: 1 } 20%,100% { transform: translateX(22px); opacity: 0 } }
    @keyframes bn-scan { 0% { transform: translateX(-400px) } 100% { transform: translateX(1680px) } }
    @media (prefers-reduced-motion: reduce) { .bn-chip, .bn-tick, .bn-dot, .bn-scan { animation: none; } }
  </style>"""
    body = f"""{style}
  <defs>
    <linearGradient id="bn-bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="#132B47"/><stop offset=".55" stop-color="#0A1626"/><stop offset="1" stop-color="#060C15"/>
    </linearGradient>
    <linearGradient id="bn-scan-g" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0" stop-color="#3FD0A4" stop-opacity="0"/><stop offset=".5" stop-color="#3FD0A4" stop-opacity=".10"/><stop offset="1" stop-color="#3FD0A4" stop-opacity="0"/>
    </linearGradient>
    <pattern id="bn-grid" width="28" height="28" patternUnits="userSpaceOnUse"><path d="M28 0 H0 V28" fill="none" stroke="#7FB2FF" stroke-opacity=".06"/></pattern>
    <clipPath id="bn-clip"><rect width="1280" height="420" rx="24"/></clipPath>
  </defs>
  <g clip-path="url(#bn-clip)">
    <rect width="1280" height="420" fill="url(#bn-bg)"/>
    <rect width="1280" height="420" fill="url(#bn-grid)"/>
    <rect class="bn-scan" x="0" y="0" width="300" height="420" fill="url(#bn-scan-g)"/>
    <circle cx="170" cy="210" r="150" fill="#3FD0A4" opacity=".08" filter="url(#np-blur)"/>
  </g>
  <g transform="translate(62 102) scale(1.68)">{mark()}
  </g>
  <g font-family="{FONT}">
    <text x="330" y="150" font-size="70" font-weight="800" letter-spacing="-2" fill="#F0F3F6">Number Passport</text>
    <text x="333" y="198" font-size="25" font-weight="500" fill="#A9C4E8">Trace any reported number to its source rows, reconcile every hop,</text>
    <text x="333" y="232" font-size="25" font-weight="500" fill="#A9C4E8">and prove it with evidence an auditor can re-check.</text>
    <text x="333" y="290" font-size="14" font-weight="700" letter-spacing="3" fill="#3FD0A4">LIVE TRACE · TOTAL EAD 2,365,768,570.80 USD · EVERY HOP RECONCILED</text>
    {"".join(parts)}
  </g>"""
    (HERE / "banner.svg").write_text(svg("0 0 1280 420", 1280, 420, body), encoding="utf-8")


if __name__ == "__main__":
    main()
