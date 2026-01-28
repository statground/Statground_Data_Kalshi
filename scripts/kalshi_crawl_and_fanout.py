#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import shutil
import hashlib
import subprocess
import datetime as dt
from pathlib import Path
import requests

# --- [1] 설정 및 초기화 --- 
START_TIME = time.time()
NOW_UTC = dt.datetime.now(dt.timezone.utc)
OWNER = os.environ.get("GITHUB_OWNER", "statground").strip()
BASE_URL = os.environ.get("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2").strip()
GH_PAT = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")

STATE_PATH = Path("kalshi_state.json")
STATS_MD_PATH = Path("KALSHI_REPO_STATS.md")
WORK_DIR = Path(".work")
WORK_REPOS_DIR = WORK_DIR / "repos"

REPO_MAX_BYTES = int(os.environ.get("REPO_MAX_BYTES", str(1 * 1024**3))) # 1GB
COMMIT_EVERY_FILES = int(os.environ.get("COMMIT_EVERY_FILES", "5000"))
FINISH_BUFFER_SEC = 15 * 60 # 15분 버퍼 

for d in [WORK_DIR, WORK_REPOS_DIR]: d.mkdir(exist_ok=True)

# --- [2] 시간 및 통계 유틸리티 --- 
def should_stop():
    """다음 KST 배차 시간(0,6,12,18시) 15분 전인지 체크""" [cite: 1]
    now = dt.datetime.now(dt.timezone.utc)
    # KST 기준 시간대를 UTC로 변환하여 체크
    sched_hours_utc = [15, 21, 3, 9] # KST 0, 6, 12, 18시
    
    current_hour = now.hour
    # 다음 예정된 UTC 시간 찾기
    next_h = min([h for h in sched_hours_utc if h > current_hour] or [min(sched_hours_utc)])
    
    target = now.replace(hour=next_h, minute=0, second=0, microsecond=0)
    if next_h <= current_hour:
        target += dt.timedelta(days=1)
        
    rem_sec = (target - now).total_seconds()
    # 다음 배차 15분 전이거나 설정된 5.5시간 예산 초과 시 종료
    return rem_sec < FINISH_BUFFER_SEC or (time.time() - START_TIME) > 19800

def update_stats_md(state):
    """실시간 통계 마크다운 파일 생성"""
    repos = state.get("repos_seen", [])
    rollover = state.get("rollover", {})
    updated_at = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    
    lines = [
        "# 📊 Kalshi Pipeline Real-time Stats",
        f"**Last Sync:** {updated_at}",
        "",
        "## 🗄️ Active Storage",
        "| Prefix | Current Index | Status |",
        "|---|---|---|",
    ]
    for prefix, idx in rollover.items():
        lines.append(f"| {prefix} | `{idx:03d}` | 🟢 Writing |")
    
    lines.append("\n## 📂 Repository List")
    for r in sorted(list(set(repos))):
        lines.append(f"- [{r}](https://github.com/{OWNER}/{r})")
    
    STATS_MD_PATH.write_text("\n".join(lines), encoding="utf-8")

# --- [3] Git 및 API 재시도 로직 ---
def api_request(url, params):
    """429 에러 대응 지수 백오프"""
    for i in range(5):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200: return r.json()
        if r.status_code == 429:
            time.sleep(2 ** (i + 1))
            continue
        break
    return None

def sync_orchestrator(msg):
    """상태와 통계를 Orchestrator 저장소에 즉시 Push"""
    repo_rel = os.environ.get('GITHUB_REPOSITORY', f"{OWNER}/Statground_Data_Kalshi")
    remote_url = f"https://x-access-token:{GH_PAT}@github.com/{repo_rel}.git"
    
    subprocess.run(["git", "remote", "set-url", "origin", remote_url])
    # 통계 파일과 상태 파일을 명시적으로 추가
    subprocess.run(["git", "add", "kalshi_state.json", "KALSHI_REPO_STATS.md"])
    
    st = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    if st.stdout.strip():
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"])
        subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"])
        subprocess.run(["git", "commit", "-m", msg])
        subprocess.run(["git", "push", "origin", "main"])

# --- [4] 메인 크롤러 루프 --- 
def main():
    state = json.loads(STATE_PATH.read_text()) if STATE_PATH.exists() else {"cursors": {}, "rollover": {}, "repos_seen": []}
    
    def checkpoint(msg):
        # 모든 열린 저장소 Flush 및 상태 저장
        state["repos_seen"] = list(set(state.get("repos_seen", [])))
        STATE_PATH.write_text(json.dumps(state, indent=2))
        update_stats_md(state) # 통계 갱신
        sync_orchestrator(msg) # 오케스트레이터 푸시

    try:
        print("Starting Crawl...")
        for kind in ["series", "event", "market"]:
            endpoint = f"/{kind if kind == 'series' else kind + 's'}"
            list_key = kind if kind == 'series' else kind + 's'
            
            while True:
                if should_stop(): # 15분 전 안전 종료 체크 
                    checkpoint(f"kalshi: {kind} safety stop")
                    return

                cursor = state["cursors"].get(kind)
                data = api_request(BASE_URL + endpoint, {"cursor": cursor} if cursor else {})
                if not data: break
                
                items = data.get(list_key, [])
                if not items: break
                
                # ... (데이터 쓰기 로직: RepoWriter 활용 부분) ...
                # 5,000개 단위 혹은 루프 종료 시 checkpoint() 호출
                
                state["cursors"][kind] = data.get("cursor") or data.get("next_cursor")
                if not state["cursors"][kind]: break
                time.sleep(0.1) # 기본 지연

        checkpoint("kalshi: batch completed")
    except Exception as e:
        checkpoint(f"kalshi: emergency backup ({str(e)[:50]})")

if __name__ == "__main__":
    main()