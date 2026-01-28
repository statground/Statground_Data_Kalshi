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
import kalshi_generate_repo_stats_md as stats_gen

# --- Configuration ---
START_TIME = time.time()
NOW_UTC = dt.datetime.now(dt.timezone.utc)
OWNER = os.environ.get("GITHUB_OWNER", "statground").strip()
BASE_URL = os.environ.get("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2").strip()
GH_PAT = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")

STATE_PATH = Path("kalshi_state.json")
WORK_DIR = Path(".work")
WORK_REPOS_DIR = WORK_DIR / "repos"

# Actions 환경 최적화 설정
REPO_MAX_BYTES = int(os.environ.get("REPO_MAX_BYTES", str(1 * 1024**3))) # 1GB
COMMIT_EVERY_FILES = int(os.environ.get("COMMIT_EVERY_FILES", "5000"))
FINISH_BUFFER_SEC = 15 * 60 

for d in [WORK_DIR, WORK_REPOS_DIR]: d.mkdir(exist_ok=True)

# --- Utilities ---
def sync_orchestrator(msg):
    """상태와 통계 리포트를 Orchestrator 저장소에 강제 Push"""
    repo_rel = os.environ.get('GITHUB_REPOSITORY', f"{OWNER}/Statground_Data_Kalshi")
    remote_url = f"https://x-access-token:{GH_PAT}@github.com/{repo_rel}.git"
    
    subprocess.run(["git", "remote", "set-url", "origin", remote_url], check=False)
    subprocess.run(["git", "add", "kalshi_state.json", "KALSHI_REPO_STATS.md"], check=False)
    
    st = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    if st.stdout.strip():
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=False)
        subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], check=False)
        subprocess.run(["git", "commit", "-m", msg], check=False)
        subprocess.run(["git", "push", "origin", "main"], check=False)

def should_stop():
    """다음 KST 스케줄(0, 6, 12, 18시) 15분 전인지 확인"""
    now = dt.datetime.now(dt.timezone.utc)
    # KST 기준 시간대를 UTC로 환산 (15, 21, 03, 09시)
    sched_utc = [15, 21, 3, 9]
    next_h = min([h for h in sched_utc if h > now.hour] or [min(sched_utc)])
    target = now.replace(hour=next_h, minute=0, second=0, microsecond=0)
    if next_h <= now.hour: target += dt.timedelta(days=1)
    
    rem_sec = (target - now).total_seconds()
    # 다음 배치 15분 전이거나 5.5시간 경과 시 종료
    return rem_sec < FINISH_BUFFER_SEC or (time.time() - START_TIME) > 19800

# --- Writer & Crawler Logic ---
class RepoWriter:
    def __init__(self, repo_name):
        self.repo = repo_name
        self.local_path = WORK_REPOS_DIR / repo_name
        self._count = 0

    def open(self):
        if not (self.local_path / ".git").exists():
            requests.post("https://api.github.com/user/repos", 
                          headers={"Authorization": f"token {GH_PAT}"}, 
                          json={"name": self.repo})
            url = f"https://x-access-token:{GH_PAT}@github.com/{OWNER}/{self.repo}.git"
            subprocess.run(["git", "clone", "--depth", "1", url, str(self.local_path)], check=False)
            subprocess.run(["git", "config", "user.name", "github-actions"], cwd=self.local_path, check=False)
            subprocess.run(["git", "config", "user.email", "actions@github.com"], cwd=self.local_path, check=False)

    def write(self, rel, obj):
        self.open()
        p = self.local_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(obj, separators=(",", ":")), encoding="utf-8")
        self._count += 1

    def flush(self):
        if self._count > 0 and self.local_path.exists():
            subprocess.run(["git", "add", "-A"], cwd=self.local_path, check=False)
            subprocess.run(["git", "commit", "-m", f"kalshi: update data {NOW_UTC}"], cwd=self.local_path, check=False)
            subprocess.run(["git", "push"], cwd=self.local_path, check=False)
            self._count = 0

def main():
    state = json.loads(STATE_PATH.read_text()) if STATE_PATH.exists() else {"cursors": {}, "rollover": {}, "repos_seen": []}
    wm = {}

    def checkpoint(msg):
        for w in wm.values(): w.flush()
        state["repos_seen"] = list(set(state.get("repos_seen", [])))
        STATE_PATH.write_text(json.dumps(state, indent=2))
        stats_gen.update_stats() # 리얼타임 파일 카운트 업데이트
        sync_orchestrator(msg)

    try:
        print("Crawl started...")
        for kind in ["series", "event", "market"]:
            endpoint = f"/{kind if kind == 'series' else kind + 's'}"
            list_key = kind if kind == 'series' else kind + 's'
            
            while True:
                if should_stop():
                    checkpoint(f"kalshi: {kind} safety timeout stop")
                    return

                cursor = state["cursors"].get(kind)
                params = {"cursor": cursor} if cursor else {}
                if kind == 'event': params["limit"] = 200
                
                resp = requests.get(BASE_URL + endpoint, params=params, timeout=60)
                if resp.status_code != 200: break
                
                data = resp.json()
                items = data.get(list_key, [])
                if not items: break

                for obj in items:
                    # (경로 파싱 로직 parse_path 등은 기존 코드 유지)
                    # 여기서는 요약된 쓰기 로직만 표시
                    repo_name = "Statground_Data_Kalshi_Series" # 예시
                    if repo_name not in wm: wm[repo_name] = RepoWriter(repo_name)
                    # wm[repo_name].write(rel, obj)
                    
                    if wm[repo_name]._count >= COMMIT_EVERY_FILES:
                        checkpoint(f"kalshi: {kind} progress")

                state["cursors"][kind] = data.get("cursor") or data.get("next_cursor")
                if not state["cursors"][kind]: break

        checkpoint("kalshi: run finished")
    except Exception as e:
        checkpoint(f"kalshi: emergency backup ({str(e)[:50]})")
        raise

if __name__ == "__main__":
    main()