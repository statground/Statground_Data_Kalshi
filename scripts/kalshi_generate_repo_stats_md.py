#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone
from pathlib import Path

def update_stats():
    """상태 파일을 읽어 실시간 통계 리포트를 생성합니다."""
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
            "| Repo Prefix | Current Index | Status |",
            "|---|:---:|---|",
        ]

        for prefix, index in rollover.items():
            lines.append(f"| {prefix} | `{index:03d}` | 🟢 Writing |")

        lines.append("\n## 📂 All Created Repositories")
        for repo in sorted(repos):
            lines.append(f"- [{repo}](https://github.com/{owner}/{repo})")

        lines.append("\n---")
        lines.append("*Note: This report is updated automatically during the crawl.*")

        out_md.write_text("\n".join(lines), encoding="utf-8")
    except Exception as e:
        print(f"Stats error: {e}")

if __name__ == "__main__":
    update_stats()