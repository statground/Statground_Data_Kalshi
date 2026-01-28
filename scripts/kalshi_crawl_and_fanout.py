#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import shutil
import hashlib
import logging
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

REPO_MAX_BYTES = int(os.environ.get("REPO_MAX_BYTES", str(1 * 1024**3))) # 1GB
COMMIT_EVERY_FILES = int(os.environ.get("COMMIT_EVERY_FILES", "5000"))
FINISH_BUFFER_SEC = int(os.environ.get("FINISH_BEFORE_NEXT_SCHEDULE_MINUTES", "15")) * 60

for d in [WORK_DIR, WORK_REPOS_DIR]: d.mkdir(exist_ok=True)

# --- Utilities ---
def run_git(cmd, cwd=None, check=True):
    p = subprocess.run(cmd, cwd=str(cwd) if cwd else None, text=True, capture_output=True)
    if check and p.returncode != 0:
        print(f"Git Cmd Failed: {' '.join(cmd)}\nError: {p.stderr}")
    return p

def sync_orchestrator(msg):
    remote_url = f"https://x-access-token:{GH_PAT}@github.com/{os.environ.get('GITHUB_REPOSITORY')}.git"
    run_git(["git", "remote", "set-url", "origin", remote_url], check=False)
    run_git(["git", "add", "kalshi_state.json", "KALSHI_REPO_STATS.md", "KALSHI_REPOS.json"], check=False)
    if run_git(["git", "status", "--porcelain"], check=False).stdout.strip():
        run_git(["git", "config", "user.name", "github-actions[bot]"], check=False)
        run_git(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], check=False)
        run_git(["git", "commit", "-m", msg], check=False)
        run_git(["git", "push", "origin", "main"], check=False)

def should_stop():
    now = dt.datetime.now(dt.timezone.utc)
    # 다음 0, 6, 12, 18시 정각 계산 
    scheds = [0, 6, 12, 18, 24]
    next_h = min([h for h in scheds if h > now.hour] or [0])
    target = now.replace(hour=next_h % 24, minute=0, second=0, microsecond=0)
    if next_h == 24 or (next_h == 0 and now.hour >= 18): target += dt.timedelta(days=1)
    return (target - now).total_seconds() < FINISH_BUFFER_SEC

# --- Sharding & Path Logic ---
def get_relpath(kind, obj):
    # 기존 복잡한 경로 생성 로직 복원
    status = "closed" if obj.get("status") == "closed" or obj.get("closed") else "open"
    # 시간 필드 파싱 (created_time 등)
    ts = obj.get("created_time") or obj.get("open_time") or time.time()
    if isinstance(ts, str): 
        try: ts = dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
        except: ts = time.time()
    d = dt.datetime.fromtimestamp(float(ts), tz=dt.timezone.utc)
    y, m = f"{d.year:04d}", f"{d.month:02d}"
    tid = str(obj.get("ticker") or obj.get("id"))
    shard = hashlib.sha1(tid.encode()).hexdigest()[:2]
    return f"{kind}s/{status}/{y}/{m}/{shard}/{tid}.json", y

# --- Writer Class ---
class RepoWriter:
    def __init__(self, repo_name):
        self.repo = repo_name
        self.local_path = WORK_REPOS_DIR / repo_name
        self._count = 0

    def ensure_repo(self):
        if not (self.local_path / ".git").exists():
            if self.local_path.exists(): shutil.rmtree(self.local_path)
            # 저장소가 없으면 생성하는 로직은 GitHub API 권한 필요하므로 여기선 클론 시도
            url = f"https://x-access-token:{GH_PAT}@github.com/{OWNER}/{self.repo}.git"
            res = run_git(["git", "clone", "--depth", "1", url, str(self.local_path)], check=False)
            if res.returncode != 0:
                print(f"Repo {self.repo} might not exist. Check auto-create settings.")
                return False
            run_git(["git", "config", "user.name", "github-actions"], cwd=self.local_path)
            run_git(["git", "config", "user.email", "actions@github.com"], cwd=self.local_path)
        return True

    def write(self, relpath, obj):
        if not self.ensure_repo(): return
        p = self.local_path / relpath
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(obj, separators=(",", ":")), encoding="utf-8")
        self._count += 1

    def flush(self):
        if self._count > 0 and self.local_path.exists():
            run_git(["git", "add", "-A"], cwd=self.local_path)
            run_git(["git", "commit", "-m", f"update data {NOW_UTC}"], cwd=self.local_path, check=False)
            run_git(["git", "push"], cwd=self.local_path, check=False)
            self._count = 0

# --- Main ---
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
        print("Crawl started...")
        for kind in ["series", "event", "market"]:
            # API 경로 결정 (/series, /events, /markets)
            api_path = f"/{kind if kind == 'series' else kind+'s'}"
            list_key = kind if kind == 'series' else kind+'s'
            
            cursor = state.get("cursors", {}).get(kind)
            while True:
                params = {"cursor": cursor} if cursor else {}
                resp = requests.get(BASE_URL + api_path, params=params, timeout=60).json()
                items = resp.get(list_key, [])
                
                for obj in items:
                    rel, year = get_relpath(kind, obj)
                    # 롤오버 인덱스 관리
                    prefix = f"Statground_Data_Kalshi_{kind.capitalize()}s_{year}" if kind != "series" else "Statground_Data_Kalshi_Series"
                    idx = state["rollover"].get(prefix, 1)
                    repo_name = f"{prefix}_{idx:03d}" if kind != "series" else prefix
                    
                    state["repos_seen"].append(repo_name)
                    if repo_name not in wm: wm[repo_name] = RepoWriter(repo_name)
                    wm[repo_name].write(rel, obj)
                    
                    # 용량 체크 및 롤오버 (단순화)
                    if wm[repo_name]._count >= COMMIT_EVERY_FILES:
                        checkpoint(f"kalshi: {kind} progress")

                cursor = resp.get("cursor") or resp.get("next_cursor")
                state.setdefault("cursors", {})[kind] = cursor
                if not cursor or should_stop(): break
            if should_stop(): break

        checkpoint("kalshi: run completed or timed out")
    except Exception as e:
        checkpoint(f"kalshi: error backup ({str(e)[:50]})")
        raise

if __name__ == "__main__":
    main()