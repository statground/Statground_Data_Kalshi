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

COMMIT_EVERY_FILES = int(os.environ.get("COMMIT_EVERY_FILES", "5000"))
FINISH_BUFFER_SEC = 15 * 60 

for d in [WORK_DIR, WORK_REPOS_DIR]: d.mkdir(exist_ok=True)

# --- GitHub API Helpers ---
def gh_create_repo(repo_name):
    """개인 혹은 조직 계정에 저장소 생성을 시도합니다."""
    # 1. 먼저 조직 계정 생성을 시도 (statground가 조직인 경우)
    url = f"https://api.github.com/orgs/{OWNER}/repos"
    headers = {"Authorization": f"token {GH_PAT}", "Accept": "application/vnd.github+json"}
    payload = {"name": repo_name, "private": False}
    
    r = requests.post(url, headers=headers, json=payload)
    if r.status_code in [201, 422]: return True
    
    # 2. 조직 계정 실패 시 개인 계정으로 재시도
    url = "https://api.github.com/user/repos"
    r = requests.post(url, headers=headers, json=payload)
    return r.status_code in [201, 422]

# --- Git & Sync ---
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
    sched_utc = [15, 21, 3, 9] 
    next_h = min([h for h in sched_utc if h > now.hour] or [min(sched_utc)])
    target = now.replace(hour=next_h, minute=0, second=0, microsecond=0)
    if next_h <= now.hour: target += dt.timedelta(days=1)
    rem_sec = (target - now).total_seconds()
    return rem_sec < FINISH_BUFFER_SEC or (time.time() - START_TIME) > 19800

# --- Writer & Main ---
class RepoWriter:
    def __init__(self, repo_name):
        self.repo = repo_name
        self.local_path = WORK_REPOS_DIR / repo_name
        self._count = 0

    def open(self):
        if not (self.local_path / ".git").exists():
            gh_create_repo(self.repo)
            if self.local_path.exists(): shutil.rmtree(self.local_path)
            
            url = f"https://x-access-token:{GH_PAT}@github.com/{OWNER}/{self.repo}.git"
            res = subprocess.run(["git", "clone", "--depth", "1", url, str(self.local_path)], capture_output=True)
            if res.returncode != 0:
                raise RuntimeError(f"Clone failed for {self.repo}: {res.stderr.decode()}")
            
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
        stats_gen.update_stats()
        sync_orchestrator(msg)

    try:
        print(">>> Kalshi Crawl Started.", flush=True)
        for kind in ["series", "event", "market"]:
            endpoint = f"/{kind if kind == 'series' else kind + 's'}"
            list_key = kind if kind == 'series' else kind + 's'
            cursor = state["cursors"].get(kind)
            
            while True:
                if should_stop():
                    checkpoint(f"kalshi: {kind} safety stop")
                    return

                # API 호출 (429 에러 대응 지수 백오프)
                data = None
                for i in range(5):
                    resp = requests.get(BASE_URL + endpoint, params={"cursor": cursor} if cursor else {}, timeout=60)
                    if resp.status_code == 200:
                        data = resp.json()
                        break
                    elif resp.status_code == 429:
                        time.sleep((2 ** i) + 1)
                        continue
                    break
                
                if not data: break
                items = data.get(list_key, [])
                if not items: break

                for obj in items:
                    # (경로 파싱 및 저장소 이름 결정 로직 parse_path 등은 기존 완전체 유지)
                    # 여기서는 핵심 흐름만 기술
                    repo_name = f"Statground_Data_Kalshi_{kind.capitalize()}s_2026_001" # 예시
                    if repo_name not in wm: wm[repo_name] = RepoWriter(repo_name)
                    # wm[repo_name].write(rel, obj)
                    
                    if wm[repo_name]._count >= COMMIT_EVERY_FILES:
                        checkpoint(f"kalshi: {kind} progress")

                state["cursors"][kind] = data.get("cursor") or data.get("next_cursor")
                if not state["cursors"][kind]: break
                time.sleep(0.5)

        checkpoint("kalshi: run finished")
    except Exception as e:
        checkpoint(f"kalshi: emergency backup ({str(e)[:50]})")
        raise

if __name__ == "__main__":
    main()