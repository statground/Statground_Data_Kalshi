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

# --- Configuration (기존 소스 파라미터 반영) ---
START_TIME = time.time()
NOW_UTC = dt.datetime.now(dt.timezone.utc)
OWNER = os.environ.get("GITHUB_OWNER", "statground").strip()
BASE_URL = os.environ.get("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2").strip()
GH_PAT = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")

STATE_PATH = Path("kalshi_state.json")
MANIFEST_PATH = Path("KALSHI_REPOS.json")
WORK_DIR = Path(".work")
WORK_REPOS_DIR = WORK_DIR / "repos"

# 파일 용량 및 커밋 주기
REPO_MAX_BYTES = int(os.environ.get("REPO_MAX_BYTES", str(1 * 1024**3))) 
COMMIT_EVERY_FILES = int(os.environ.get("COMMIT_EVERY_FILES", "5000"))
FINISH_BUFFER_SEC = int(os.environ.get("FINISH_BEFORE_NEXT_SCHEDULE_MINUTES", "15")) * 60

# 백필 설정 (기존 소스 참고)
EVENT_BACKFILL_SEC = int(os.environ.get("KALSHI_EVENT_BACKFILL_SEC", "172800")) # 48h

for d in [WORK_DIR, WORK_REPOS_DIR]: d.mkdir(exist_ok=True)

# --- GitHub API & Git Helpers ---
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
    # 다음 KST 배차 시간 계산
    scheds = [0, 6, 12, 18, 24]
    next_h = min([h for h in scheds if h > now.hour] or [0])
    target = now.replace(hour=next_h % 24, minute=0, second=0, microsecond=0)
    if next_h == 24 or (next_h == 0 and now.hour >= 18): target += dt.timedelta(days=1)
    
    rem_sec = (target - now).total_seconds()
    if rem_sec < FINISH_BUFFER_SEC:
        print(f"Safety Stop: {rem_sec/60:.1f} min until next schedule.")
        return True
    return False

# --- Path & Writer ---
def get_path_info(kind, obj):
    # 기존 소스의 정교한 시간 파싱 적용
    ts_val = obj.get("created_time") or obj.get("open_time") or obj.get("close_time") or time.time()
    if isinstance(ts_val, str):
        try: ts = dt.datetime.fromisoformat(ts_val.replace("Z", "+00:00")).timestamp()
        except: ts = time.time()
    else: ts = float(ts_val)
    
    d = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
    y, m = f"{d.year:04d}", f"{d.month:02d}"
    status = "closed" if obj.get("status") == "closed" or obj.get("closed") else "open"
    tid = str(obj.get("ticker") or obj.get("id"))
    shard = hashlib.sha1(tid.encode()).hexdigest()[:2]
    
    rel = f"{kind}s/{status}/{y}/{m}/{shard}/{tid}.json"
    return rel, y

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
        print(f"Crawl Starting... Owner: {OWNER}")
        for kind in ["series", "event", "market"]:
            api_path = f"/{kind if kind == 'series' else kind+'s'}"
            list_key = kind if kind == 'series' else kind+'s'
            cursor = state.get("cursors", {}).get(kind)
            
            print(f"Fetching {kind} (Cursor: {cursor})")
            while True:
                params = {"cursor": cursor} if cursor else {}
                # [중요] 필터링 파라미터가 너무 좁으면 데이터가 안 올 수 있음
                resp = requests.get(BASE_URL + api_path, params=params, timeout=60).json()
                items = resp.get(list_key, [])
                
                print(f"  > Received {len(items)} {kind} items.")
                if not items: break
                
                for obj in items:
                    rel, year = get_path_info(kind, obj)
                    prefix = f"Statground_Data_Kalshi_{kind.capitalize()}s_{year}" if kind != "series" else "Statground_Data_Kalshi_Series"
                    idx = state["rollover"].get(prefix, 1)
                    repo_name = f"{prefix}_{idx:03d}" if kind != "series" else prefix
                    
                    if repo_name not in state["repos_seen"]: state["repos_seen"].append(repo_name)
                    if repo_name not in wm: wm[repo_name] = RepoWriter(repo_name)
                    
                    wm[repo_name].write(rel, obj)
                    
                    if wm[repo_name]._count >= COMMIT_EVERY_FILES:
                        checkpoint(f"kalshi: {kind} progress")

                cursor = resp.get("cursor") or resp.get("next_cursor")
                state.setdefault("cursors", {})[kind] = cursor
                if not cursor or should_stop(): break
                time.sleep(0.05) # Rate limit 방지 
            if should_stop(): break

        checkpoint("kalshi: run finished")
    except Exception as e:
        checkpoint(f"kalshi: error backup ({str(e)[:50]})")
        raise

if __name__ == "__main__":
    main()