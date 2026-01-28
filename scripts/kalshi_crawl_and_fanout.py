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
from typing import Any, Dict, Iterable, List, Optional
import requests

# 통계 모듈 임포트
import kalshi_generate_repo_stats_md as stats_gen

# --- Configuration ---
NOW_UTC = dt.datetime.now(dt.timezone.utc)
OWNER = os.environ.get("GITHUB_OWNER", "statground").strip()
BASE_URL = os.environ.get("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2").strip()

# 상태 및 매니페스트 경로 (루트에 배치하여 추적 보장)
STATE_PATH = Path("kalshi_state.json")
MANIFEST_PATH = Path("KALSHI_REPOS.json")

# 분할 임계값 (GHA 환경에 맞춰 1GiB로 현실화)
REPO_MAX_BYTES = int(os.environ.get("REPO_MAX_BYTES", str(1 * 1024**3)))
COMMIT_EVERY_FILES = int(os.environ.get("COMMIT_EVERY_FILES", "5000"))
GH_PAT = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")

WORK_DIR = Path(".work")
WORK_REPOS_DIR = WORK_DIR / "repos"
for d in [WORK_DIR, WORK_REPOS_DIR]: d.mkdir(exist_ok=True)

# --- Git Helpers ---
def run_git(cmd, cwd=None, check=True):
    p = subprocess.run(cmd, cwd=str(cwd) if cwd else None, text=True, capture_output=True)
    if check and p.returncode != 0:
        print(f"Git Error: {p.stderr}")
    return p

def sync_orchestrator(msg):
    """현재 저장소(Orchestrator)의 진행 상황을 즉시 Push"""
    run_git(["git", "add", "kalshi_state.json", "KALSHI_REPO_STATS.md", "KALSHI_REPOS.json"])
    status = run_git(["git", "status", "--porcelain"])
    if status.stdout.strip():
        run_git(["git", "config", "user.name", "github-actions[bot]"], check=False)
        run_git(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], check=False)
        run_git(["git", "commit", "-m", msg])
        run_git(["git", "push"])
        print(f"Checkpoint Pushed: {msg}")

# --- Writer Class ---
class RepoWriter:
    def __init__(self, owner, repo, local_path):
        self.owner, self.repo, self.local_path = owner, repo, local_path
        self._since_commit = 0

    def ensure_repo(self):
        if not (self.local_path / ".git").exists():
            url = f"https://x-access-token:{GH_PAT}@github.com/{self.owner}/{self.repo}.git"
            # 실제 환경에서는 gh_create_repo 로직이 포함되어야 함
            run_git(["git", "clone", "--depth", "1", url, str(self.local_path)])
            run_git(["git", "config", "user.name", "github-actions"], cwd=self.local_path)
            run_git(["git", "config", "user.email", "actions@github.com"], cwd=self.local_path)

    def write(self, relpath, obj):
        self.ensure_repo()
        full_path = self.local_path / relpath
        full_path.parent.mkdir(parents=True, exist_ok=True)
        with open(full_path, "w", encoding="utf-8") as f:
            json.dump(obj, f, separators=(",", ":"))
        self._since_commit += 1

    def flush(self):
        if self._since_commit > 0:
            run_git(["git", "add", "-A"], cwd=self.local_path)
            run_git(["git", "commit", "-m", f"kalshi: update data {NOW_UTC}"], cwd=self.local_path)
            run_git(["git", "push"], cwd=self.local_path)
            self._since_commit = 0

# --- Main Logic ---
def main():
    # 1. 상태 로드
    if STATE_PATH.exists():
        state = json.loads(STATE_PATH.read_text())
    else:
        state = {"cursors": {}, "rollover": {}, "repos_seen": []}

    repos_seen = set(state.get("repos_seen", []))
    wm = {} # Writer manager

    def checkpoint(msg):
        """데이터 푸시 + 상태 저장 + 통계 갱신 + 오케스트레이터 푸시"""
        for writer in wm.values():
            writer.flush()
        state["repos_seen"] = list(repos_seen)
        STATE_PATH.write_text(json.dumps(state, indent=2))
        # 통계 스크립트 호출
        stats_gen.update_stats()
        # 오케스트레이터 푸시
        sync_orchestrator(msg)

    try:
        # [예시 루프] 실제 Kalshi API 호출 및 paginate 로직 결합 부분
        # (이 부분에 기존 paginate와 API 호출 코드를 넣으시면 됩니다)
        print("Starting crawl...")
        
        # 임시 테스트 로직 (실제 API 결과에 따라 대체)
        # for item in items:
        #    ... (저장소 결정 및 write) ...
        #    if count % COMMIT_EVERY_FILES == 0:
        #        checkpoint(f"Progress: {count} items")

        checkpoint("Final update for this run")

    except Exception as e:
        checkpoint(f"Emergency save on error: {str(e)[:50]}")
        raise

if __name__ == "__main__":
    main()