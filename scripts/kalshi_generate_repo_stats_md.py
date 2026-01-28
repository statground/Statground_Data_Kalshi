#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone
from pathlib import Path

def update_stats():
    state_path = Path("kalshi_state.json")
    out_md = Path("KALSHI_REPO_STATS.md")
    owner = os.environ.get("GITHUB_OWNER", "statground")

    if not state_path.exists():
        return

    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        repos = state.get("repos_seen", [])
        rollover = state.get("rollover", {})
        
        lines = [
            "# 📊 Kalshi Data Pipeline Stats",
            f"**Last Sync (UTC):** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
            f"**Target Owner:** `{owner}`",
            "",
            "## 🗄️ Active Storage (Rollover)",
            "| Repo Prefix | Index | Status |",
            "|---|:---:|---|",
        ]

        for prefix, index in rollover.items():
            lines.append(f"| {prefix} | `{index:03d}` | 🟢 Active |")

        lines.append("\n## 📂 Created Repositories")
        for repo in sorted(list(set(repos))):
            lines.append(f"- [{repo}](https://github.com/{owner}/{repo})")

        out_md.write_text("\n".join(lines), encoding="utf-8")
    except Exception as e:
        print(f"Stats Error: {e}")

if __name__ == "__main__":
    update_stats()