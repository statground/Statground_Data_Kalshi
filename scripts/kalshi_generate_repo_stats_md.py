#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone
from pathlib import Path

def count_files(directory):
    total = 0
    if not os.path.exists(directory): return 0
    for root, dirs, files in os.walk(directory):
        if '.git' in root: continue
        total += len(files)
    return total

def update_stats():
    state_path = Path("kalshi_state.json")
    out_md = Path("KALSHI_REPO_STATS.md")
    repos_base = Path(".work/repos")
    owner = os.environ.get("GITHUB_OWNER", "statground")

    if not state_path.exists(): return

    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        repos_seen = state.get("repos_seen", [])
        
        lines = [
            "# 📊 Kalshi Pipeline Real-time Stats",
            f"**마지막 갱신 (UTC):** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## 🗄️ 데이터 저장소별 수집 현황",
            "| 저장소 명 | 파일 개수 (로컬) | 상태 |",
            "|---|---:|---|",
        ]

        grand_total = 0
        for repo in sorted(list(set(repos_seen))):
            f_count = count_files(repos_base / repo)
            grand_total += f_count
            lines.append(f"| [{repo}](https://github.com/{owner}/{repo}) | `{f_count:,}` | 🟢 활성 |")

        lines.append(f"| **전체 합계** | **`{grand_total:,}`** | |")
        out_md.write_text("\n".join(lines), encoding="utf-8")
    except Exception as e:
        print(f"Stats Error: {e}")

if __name__ == "__main__":
    update_stats()