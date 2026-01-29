#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
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

COMMIT_EVERY_FILES = 5000 
FINISH_BUFFER_SEC = 15 * 60 

for d in [WORK_DIR, WORK_REPOS_DIR]: d.mkdir(exist_ok=True)

# --- GitHub & Logic Helpers ---
def sync_orchestrator(msg):
    repo_rel = os.environ.get('GITHUB_REPOSITORY', f"{OWNER}/Statground_Data_Kalshi")
    remote_url = f"https://x-access-token:{GH_PAT}@github.com/{repo_rel}.git"
    subprocess.run(["git", "remote", "set-url", "origin", remote_url], check=False)
    subprocess.run(["git", "add", "kalshi_state.json", "KALSHI_REPO_STATS.md"], check=False)
    if subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True).stdout.strip():
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=False)
        subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], check=False)
        subprocess.run(["git", "commit", "-m", msg], check=False)
        subprocess.run(["git", "push", "origin", "main"], check=False)

def should_stop():
    now = dt.datetime.now(dt.timezone.utc)
    # KST 배차 시간 (0, 6, 12, 18시) 기준 UTC 체크
    sched_utc = [15, 21, 3, 9] 
    next_h = min([h for h in sched_utc if h > now.hour] or [min(sched_utc)])
    target = now.replace(hour=next_h, minute=0, second=0, microsecond=0)
    if next_h <= now.hour: target += dt.timedelta(days=1)
    return (target - now).total_seconds() < FINISH_BUFFER_SEC

# --- Data Path Logic ---
def get_path_info(kind, obj):
    # 날짜 필드 우선순위: created -> open -> close -> now
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
    return f"{kind}s/{status}/{y}/{m}/{shard}/{tid}.json", y

# --- Writer Class ---
class RepoWriter:
    def __init__(self, repo_name):
        self.repo = repo_name
        self.local_path = WORK_REPOS_DIR / repo_name
        self._count = 0

    def open(self):
        if not (self.local_path / ".git").exists():
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
            subprocess.run(["git", "commit", "-m", f"kalshi: update {NOW_UTC}"], cwd=self.local_path, check=False)
            subprocess.run(["git", "push"], cwd=self.local_path, check=False)
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
        print(">>> Crawler Restarted with Enhanced Pagination", flush=True)
        
        # 순서대로 처리
        for kind in ["series", "event", "market"]:
            endpoint = f"/{kind if kind == 'series' else kind + 's'}"
            list_key = kind if kind == 'series' else kind + 's'
            cursor = state["cursors"].get(kind)
            
            print(f"--- Processing {kind.upper()} ---", flush=True)
            
            while True:
                if should_stop():
                    checkpoint(f"kalshi: {kind} stop for next schedule")
                    return

                params = {"cursor": cursor} if cursor else {}
                if kind == 'event': params["limit"] = 200 # 이벤트는 크게
                if kind == 'market': params["limit"] = 1000 # 마켓은 최대치 시도
                
                resp_raw = requests.get(BASE_URL + endpoint, params=params, timeout=60)
                if resp_raw.status_code == 429:
                    print("Rate limit hit, waiting 30s...", flush=True)
                    time.sleep(30)
                    continue
                if resp_raw.status_code != 200: break
                
                data = resp_raw.json()
                items = data.get(list_key, [])
                
                if items:
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

                # [중요] Cursor 업데이트 로직 강화
                next_cursor = data.get("cursor") or data.get("next_cursor")
                
                # 다음 커서가 현재와 같거나 없으면 종료
                if not next_cursor or next_cursor == cursor:
                    print(f"Finished {kind}: No more cursors.", flush=True)
                    break
                
                cursor = next_cursor
                state["cursors"][kind] = cursor
                
                # API 부하 방지
                time.sleep(0.5)

        checkpoint("kalshi: full run finished")
    except Exception as e:
        checkpoint(f"kalshi: emergency exit ({str(e)[:50]})")
        raise

if __name__ == "__main__":
    main()