#!/usr/bin/env python3
import json
import os
from datetime import datetime, timezone
from pathlib import Path

def count_files(directory):
    """디렉토리 내의 모든 파일 개수를 재귀적으로 계산"""
    return sum([len(files) for r, d, files in os.walk(directory) if '.git' not in r])

def update_stats():
    state_path = Path("kalshi_state.json")
    out_md = Path("KALSHI_REPO_STATS.md")
    repos_dir = Path(".work/repos")
    owner = os.environ.get("GITHUB_OWNER", "statground")

    if not state_path.exists(): return

    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
        repos_seen = state.get("repos_seen", [])
        rollover = state.get("rollover", {})
        
        lines = [
            "# 📊 Kalshi Pipeline Real-time Stats",
            f"**마지막 갱신 (UTC):** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## 🗄️ 데이터 수집 현황 (Shard 별)",
            "| 저장소 명 | 파일 개수 (로컬) | 상태 |",
            "|---|---:|---|",
        ]

        total_all = 0
        for repo in sorted(list(set(repos_seen))):
            repo_path = repos_dir / repo
            f_count = count_files(repo_path) if repo_path.exists() else 0
            total_all += f_count
            lines.append(f"| [{repo}](https://github.com/{owner}/{repo}) | `{f_count:,}` | 🟢 수집 중 |")

        lines.append(f"| **합계** | **`{total_all:,}`** | |")
        lines.append("\n---")
        lines.append("*참고: 5,000개 단위로 저장소에 Push되며, 위 수치는 현재 작업 서버의 로컬 집계량입니다.*")

        out_md.write_text("\n".join(lines), encoding="utf-8")
    except Exception as e:
        print(f"Stats Error: {e}")

if __name__ == "__main__":
    update_stats()