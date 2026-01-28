#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone
from pathlib import Path

# 설정
OWNER = os.environ.get("GITHUB_OWNER", "statground")
STATE_PATH = Path("kalshi_state.json")
OUT_MD = Path("KALSHI_REPO_STATS.md")

def update_stats():
    """상태 파일을 기반으로 실시간 MD 리포트 생성"""
    if not STATE_PATH.exists():
        return

    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        repos_seen = state.get("repos_seen", [])
        rollover = state.get("rollover", {})
        
        lines = [
            "# 📊 Kalshi Data Pipeline Stats",
            f"**Last Sync (UTC):** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
            f"**Target Owner:** `{OWNER}`",
            "",
            "## 🗄️ Active Storage (Rollover)",
            "| Repo Prefix | Current Index | Status |",
            "|---|:---:|---|",
        ]

        for prefix, index in rollover.items():
            lines.append(f"| {prefix} | `{index:03d}` | 🟢 Writing |")

        lines.append("\n## 📂 All Created Repositories")
        for repo in sorted(repos_seen):
            lines.append(f"- [{repo}](https://github.com/{OWNER}/{repo})")

        lines.append("\n---")
        lines.append("*Note: This report updates every 5,000 files during the crawl.*")

        OUT_MD.write_text("\n".join(lines), encoding="utf-8")
        print(f"Successfully updated {OUT_MD}")
        
    except Exception as e:
        print(f"Error in stats generation: {e}")

if __name__ == "__main__":
    update_stats()