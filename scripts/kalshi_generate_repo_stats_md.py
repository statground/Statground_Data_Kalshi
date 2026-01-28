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
            "# 📊 Kalshi Pipeline Real-time Stats",
            f"**마지막 갱신 (UTC):** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
            f"**대상 계정:** `{owner}`",
            "",
            "## 🗄️ 활성 저장소 인덱스",
            "| 데이터 구분 | 현재 인덱스 | 상태 |",
            "|---|:---:|---|",
        ]

        for prefix, index in rollover.items():
            lines.append(f"| {prefix} | `{index:03d}` | 🟢 수집 중 |")

        lines.append("\n## 📂 전체 데이터 저장소 목록")
        for repo in sorted(list(set(repos))):
            lines.append(f"- [{repo}](https://github.com/{owner}/{repo})")

        out_md.write_text("\n".join(lines), encoding="utf-8")
    except Exception as e:
        print(f"Stats Error: {e}")

if __name__ == "__main__":
    update_stats()