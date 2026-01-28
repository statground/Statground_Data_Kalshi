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
MANIFEST_PATH = Path("KALSHI_REPOS.json")
WORK_DIR = Path(".work")
WORK_REPOS_DIR = WORK_DIR / "repos"

REPO_MAX_BYTES = int(os.environ.get("REPO_MAX_BYTES", str(1 * 1024**3))) 
COMMIT_EVERY_FILES = int(os.environ.get("COMMIT_EVERY_FILES", "5000"))
FINISH_BUFFER_SEC = int(os.environ.get("FINISH_BEFORE_NEXT_SCHEDULE_MINUTES", "15")) * 60

for d in [WORK_DIR, WORK_REPOS_DIR]: d.mkdir(exist_ok=True)

# --- Git & GitHub Helpers ---
def gh_create_repo(repo_name):
    url = "https://api.github.com/user/repos"
    headers = {"Authorization": f"token {GH_PAT}", "Accept": "application/vnd.github+json"}
    payload = {"name": repo_name, "private": False, "description": f"Kalshi Data: {repo_name}"}
    r = requests.post(url, headers=headers, json=payload)
    return r.status_code in [201, 422]

def run_git(cmd, cwd=None):
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, text=True, capture_output=True)

def sync_orchestrator(msg):
    repo_rel = os.environ.get('GITHUB_REPOSITORY', f"{OWNER}/Statground_Data_Kalshi")
    remote_url = f"https://x-access-token:{GH_PAT}@github.com/{repo_rel}.git"
    run_git(["git", "remote", "set-url", "origin", remote_url])
    run_git(["git", "add", "kalshi_state.json", "KALSHI_REPO_STATS.md", "KALSHI_REPOS.json"])
    if run_git(["git", "status", "--porcelain"]).stdout.strip():
        run_git(["git", "config", "user.name", "github-actions[bot]"])
        run_git(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"])
        run_git(["git", "commit", "-m", msg])
        run_git(["git", "push", "origin", "main"])

def should_stop():
    now = dt.datetime.now(dt.timezone.utc)
    scheds = [0, 6, 12, 18, 24]
    next_h = min([h for h in scheds if h > now.hour] or [0])
    target = now.replace(hour=next_h % 24, minute=0, second=0, microsecond=0)
    if next_h == 24 or (next_h == 0 and now.hour >= 18): target += dt.timedelta(days=1)
    return (target - now).total_seconds() < FINISH_BUFFER_SEC

# --- Data Writer ---
class RepoWriter:
    def __init__(self, repo_name):
        self.repo = repo_name
        self.local_path = WORK_REPOS_DIR / repo_name
        self._count = 0

    def open(self):
        if not (self.local_path / ".git").exists():
            gh_create_repo(self.repo)
            url = f"https://x-access-token:{GH_PAT}@github.com/{OWNER}/{self.repo}.git"
            run_git(["git", "clone", "--depth", "1", url, str(self.local_path)])
            run_git(["git", "config", "user.name", "github-actions"], cwd=self.local_path)
            run_git(["git", "config", "user.email", "actions@github.com"], cwd=self.local_path)

    def write(self, rel, obj):
        self.open()
        p = self.local_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(obj, separators=(",", ":")), encoding="utf-8")
        self._count += 1

    def flush(self):
        if self._count > 0 and self.local_path.exists():
            run_git(["git", "add", "-A"], cwd=self.local_path)
            run_git(["git", "commit", "-m", f"kalshi: update data {NOW_UTC}"], cwd=self.local_path)
            run_git(["git", "push"], cwd=self.local_path)
            self._count = 0

# --- Helper Functions ---
def parse_path(kind, obj):
    # 날짜 추출
    ts_val = obj.get("created_time") or obj.get("open_time") or obj.get("last_updated_time") or time.time()
    if isinstance(ts_val, str):
        try: ts = dt.datetime.fromisoformat(ts_val.replace("Z", "+00:00")).timestamp()
        except: ts = time.time()
    else: ts = float(ts_val)
    d = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
    y, m = f"{d.year:04d}", f"{d.month:02d}"
    
    # 상태 및 경로 생성
    status = "closed" if obj.get("status") == "closed" or obj.get("closed") else "open"
    tid = str(obj.get("ticker") or obj.get("id"))
    shard = hashlib.sha1(tid.encode()).hexdigest()[:2]
    return f"{kind}s/{status}/{y}/{m}/{shard}/{tid}.json", y

# --- Main Crawler ---
def main():
    state = json.loads(STATE_PATH.read_text()) if STATE_PATH.exists() else {"cursors": {}, "rollover": {}, "repos_seen": []}
    wm = {}

    def checkpoint(msg):
        for w in wm.values(): w.flush()
        state["repos_seen"] = list(set(state.get("repos_seen", [])))
        STATE_PATH.write_text(json.dumps(state, indent=2))
        stats_gen.update_stats()
        sync_orchestrator(msg)

    try:
        print(f"Starting Crawl... State Reset: {not STATE_PATH.exists()}")
        
        # 순서: Series -> Events -> Markets
        for kind in ["series", "event", "market"]:
            # API 경로 및 키 설정
            api_endpoint = f"/{kind if kind == 'series' else kind + 's'}"
            list_key = kind if kind == 'series' else kind + 's'
            cursor = state["cursors"].get(kind)
            
            print(f"--- Processing {kind.upper()} (Cursor: {cursor}) ---")
            
            while True:
                # API 호출
                url = BASE_URL + api_endpoint
                params = {"cursor": cursor} if cursor else {}
                if kind == 'event': params["limit"] = 200 # 이벤트는 보통 크게 가져옴
                
                resp_raw = requests.get(url, params=params, timeout=60)
                if resp_raw.status_code != 200:
                    print(f"  [Error] API Returned {resp_raw.status_code}: {resp_raw.text}")
                    break
                
                resp = resp_raw.json()
                items = resp.get(list_key, [])
                print(f"  [Info] Received {len(items)} {kind} items.")
                
                if not items:
                    break
                
                for obj in items:
                    rel, year = parse_path(kind, obj)
                    
                    # 저장소 이름 결정
                    if kind == "series":
                        repo_name = "Statground_Data_Kalshi_Series"
                    else:
                        prefix = f"Statground_Data_Kalshi_{kind.capitalize()}s_{year}"
                        idx = state["rollover"].get(prefix, 1)
                        repo_name = f"{prefix}_{idx:03d}"
                    
                    if repo_name not in state["repos_seen"]:
                        state["repos_seen"].append(repo_name)
                    
                    if repo_name not in wm:
                        wm[repo_name] = RepoWriter(repo_name)
                    
                    wm[repo_name].write(rel, obj)
                    
                    # 커밋 주기 체크
                    if wm[repo_name]._count >= COMMIT_EVERY_FILES:
                        checkpoint(f"kalshi: {kind} progress checkpoint")

                # 다음 페이지 확인
                cursor = resp.get("cursor") or resp.get("next_cursor")
                state["cursors"][kind] = cursor
                
                if not cursor or should_stop():
                    break
                
                time.sleep(0.1) # 안전을 위한 짧은 지연
                
            if should_stop():
                print(">>> Time buffer reached. Stopping crawl.")
                break
        
        checkpoint("kalshi: run finished successfully")
        
    except Exception as e:
        print(f"!!! Fatal Error: {e}")
        checkpoint(f"kalshi: emergency backup on error")
        raise

if __name__ == "__main__":
    main()