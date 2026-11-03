"""The Claude Code plugin files stay consistent with the package."""

from __future__ import annotations

import json
from pathlib import Path

from number_passport import __version__

ROOT = Path(__file__).resolve().parents[1]


def test_plugin_manifest_matches_package() -> None:
    plugin = json.loads((ROOT / ".claude-plugin" / "plugin.json").read_text())
    assert plugin["name"] == "number-passport" and plugin["version"] == __version__
    market = json.loads((ROOT / ".claude-plugin" / "marketplace.json").read_text())
    assert [p["name"] for p in market["plugins"]] == ["number-passport"]
    mcp = json.loads((ROOT / ".mcp.json").read_text())
    args = mcp["mcpServers"]["number-passport"]["args"]
    assert args[-2:] == ["passport", "mcp"] and f">={__version__}" in args[1]


def test_skill_frontmatter() -> None:
    text = (ROOT / "skills" / "number-passport" / "SKILL.md").read_text()
    head = text.split("---")[1]
    assert "name: number-passport" in head and "description:" in head
    assert len(head) < 1200
