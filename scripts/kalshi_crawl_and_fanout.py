#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import shutil
import subprocess
import datetime as dt
from pathlib import Path
import requests
import kalshi_generate_repo_stats_md as stats_gen

# --- Configuration ---
OWNER = os.environ.get("GITHUB_OWNER", "statground").strip()
BASE_URL = os.environ.get("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2").strip()
STATE_PATH = Path("kalshi_state.json")
COMMIT_EVERY_FILES = int(os.environ.get("COMMIT_EVERY_FILES", "5000"))
GH_PAT = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")

# --- Git Helpers ---
def run_git(cmd, cwd=None):
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, text=True, capture_output=True)

def sync_orchestrator(msg):
    """상태 및 통계를 Orchestrator에 Push (에러 발생 시 무시하고 진행)"""
    # [중요] Actions 내부에서 Push를 위해 원격 주소 재설정
    remote_url = f"https://x-access-token:{GH_PAT}@github.com/{os.environ.get('GITHUB_REPOSITORY')}.git"
    run_git(["git", "remote", "set-url", "origin", remote_url])
    
    run_git(["git", "add", "kalshi_state.json", "KALSHI_REPO_STATS.md", "KALSHI_REPOS.json"])
    status = run_git(["git", "status", "--porcelain"])
    
    if status.stdout.strip():
        run_git(["git", "config", "user.name", "github-actions[bot]"])
        run_git(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"])
        run_git(["git", "commit", "-m", msg])
        # push 실패 시(충돌 등) 워크플로우 중단 방지를 위해 check=False와 유사하게 처리
        res = run_git(["git", "push", "origin", "main"])
        if res.returncode != 0:
            print(f"Orchestrator push deferred (likely conflict): {res.stderr}")

# --- Main Logic ---
def main():
    # 1. 상태 로드
    state = json.loads(STATE_PATH.read_text()) if STATE_PATH.exists() else {"cursors": {}, "rollover": {}, "repos_seen": []}
    
    # [핵심] 실제 크롤링 로직은 기존 소스(scripts/kalshi_crawl_and_fanout.py)의 
    # paginate 함수와 연동되어야 합니다. 아래는 루프 내 체크포인트 예시입니다.
    
    def checkpoint(count, mode="progress"):
        state["repos_seen"] = list(set(state.get("repos_seen", [])))
        STATE_PATH.write_text(json.dumps(state, indent=2))
        stats_gen.update_stats()
        sync_orchestrator(f"kalshi: {mode} ({count:,})")

    try:
        # 이 부분에 기존 크롤링 루프를 삽입하십시오.
        # 루프 내부에서 일정 주기마다 checkpoint(n)을 호출합니다.
        print("Crawl started...")
        checkpoint(0, "started")
        
    except Exception as e:
        print(f"Error during crawl: {e}")
        checkpoint(0, "error_backup")
        raise

if __name__ == "__main__":
    main()