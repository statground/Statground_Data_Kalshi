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
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple
import requests

# 통계 모듈 임포트 (파일이 같은 위치에 있어야 함)
import kalshi_generate_repo_stats_md as stats_gen

# -----------------------------
# 1. Config & Globals
# -----------------------------
NOW_UTC = dt.datetime.now(dt.timezone.utc)
OWNER = os.environ.get("GITHUB_OWNER", "statground").strip()
BASE_URL = os.environ.get("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2").strip()

STATE_PATH = Path("kalshi_state.json")
MANIFEST_PATH = Path("KALSHI_REPOS.json")
WORK_DIR = Path(".work")
WORK_REPOS_DIR = WORK_DIR / "repos"

REPO_MAX_BYTES = int(os.environ.get("REPO_MAX_BYTES", str(1 * 1024**3))) # 1GB 분할
COMMIT_EVERY_FILES = int(os.environ.get("COMMIT_EVERY_FILES", "5000"))
GH_PAT = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")

for d in [WORK_DIR, WORK_REPOS_DIR]: d.mkdir(exist_ok=True)

# -----------------------------
# 2. Git & Orchestrator Sync
# -----------------------------
def run_git(cmd, cwd=None):
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, text=True, capture_output=True)

def sync_orchestrator(msg):
    """현재 Orchestrator(본 저장소)의 상태와 통계를 즉시 Push"""
    remote_url = f"https://x-access-token:{GH_PAT}@github.com/{os.environ.get('GITHUB_REPOSITORY')}.git"
    run_git(["git", "remote", "set-url", "origin", remote_url])
    
    run_git(["git", "add", "kalshi_state.json", "KALSHI_REPO_STATS.md", "KALSHI_REPOS.json"])
    st = run_git(["git", "status", "--porcelain"])
    if st.stdout.strip():
        run_git(["git", "config", "user.name", "github-actions[bot]"])
        run_git(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"])
        run_git(["git", "commit", "-m", msg])
        res = run_git(["git", "push", "origin", "main"])
        if res.returncode == 0:
            print(f">>> Orchestrator Sync Success: {msg}")
        else:
            print(f">>> Orchestrator Sync Deferred: {res.stderr}")

# -----------------------------
# 3. Writer & Sharding Logic
# -----------------------------
class RepoWriter:
    def __init__(self, owner, repo, local_path):
        self.owner = owner
        self.repo = repo
        self.local_path = local_path
        self._since_commit = 0

    def ensure_open(self):
        if not (self.local_path / ".git").exists():
            url = f"https://x-access-token:{GH_PAT}@github.com/{self.owner}/{self.repo}.git"
            if self.local_path.exists(): shutil.rmtree(self.local_path)
            # 깃허브 API로 저장소 존재 확인/생성 로직은 기존 소스 활용 권장
            run_git(["git", "clone", "--depth", "1", url, str(self.local_path)])
            run_git(["git", "config", "user.name", "github-actions"], cwd=self.local_path)
            run_git(["git", "config", "user.email", "actions@github.com"], cwd=self.local_path)

    def write_json(self, relpath, obj):
        self.ensure_open()
        out = self.local_path / relpath
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(obj, f, separators=(",", ":"))
        self._since_commit += 1

    def flush(self):
        if self._since_commit > 0:
            run_git(["git", "add", "-A"], cwd=self.local_path)
            run_git(["git", "commit", "-m", f"kalshi: update data {NOW_UTC}"], cwd=self.local_path)
            run_git(["git", "push"], cwd=self.local_path)
            self._since_commit = 0

# -----------------------------
# 4. API & Pagination (기존 로직 복구)
# -----------------------------
SESSION = requests.Session()
def api_get(path, params=None):
    r = SESSION.get(BASE_URL.rstrip("/") + path, params=params or {}, timeout=60)
    r.raise_for_status()
    return r.json()

def paginate(path, list_key, state, state_name):
    cursor = state.get("cursors", {}).get(state_name)
    while True:
        data = api_get(path, {"cursor": cursor} if cursor else {})
        items = data.get(list_key, [])
        for it in items: yield it
        cursor = data.get("cursor") or data.get("next_cursor")
        state.setdefault("cursors", {})[state_name] = cursor
        if not cursor: break

# -----------------------------
# 5. Main Loop
# -----------------------------
def main():
    if STATE_PATH.exists():
        state = json.loads(STATE_PATH.read_text())
    else:
        state = {"cursors": {}, "rollover": {}, "repos_seen": []}
    
    repos_seen = set(state.get("repos_seen", []))
    wm = {}

    def checkpoint(kind, count):
        for w in wm.values(): w.flush()
        state["repos_seen"] = list(repos_seen)
        STATE_PATH.write_text(json.dumps(state, indent=2))
        stats_gen.update_stats() # 통계 업데이트
        sync_orchestrator(f"kalshi: {kind} progress ({count:,})")

    try:
        print("Crawl started...")
        
        # 예: Events 수집 루프 (기존 relpath 로직 등은 생략됨, 필요시 추가)
        count = 0
        for o in paginate("/events", "events", state, "events"):
            # 임시: 2026_001 저장소 결정 로직 (pick_repo_for 필요)
            repo_name = f"Statground_Data_Kalshi_Events_Current_001"
            repos_seen.add(repo_name)
            
            if repo_name not in wm:
                wm[repo_name] = RepoWriter(OWNER, repo_name, WORK_REPOS_DIR / repo_name)
            
            # 파일 쓰기 (기존 shard_prefix 등 경로 로직 적용 권장)
            eid = o.get("ticker", "unknown")
            wm[repo_name].write_json(f"events/{eid}.json", o)
            
            count += 1
            if count % COMMIT_EVERY_FILES == 0:
                checkpoint("events", count)
        
        checkpoint("final", count)
        print(f"Done. Processed {count} items.")

    except Exception as e:
        checkpoint("error_backup", 0)
        print(f"Fatal error: {e}")
        raise

if __name__ == "__main__":
    main()