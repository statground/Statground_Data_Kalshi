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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
import requests

# -----------------------------
# Config & Paths
# -----------------------------
NOW_UTC = dt.datetime.now(dt.timezone.utc)
OWNER = os.environ.get("GITHUB_OWNER", "statground").strip()
BASE_URL = os.environ.get("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2").strip()

# 상태 파일 위치 (절대 ignore 되지 않도록 루트에 배치)
STATE_PATH = Path("kalshi_state.json")
MANIFEST_PATH = Path("KALSHI_REPOS.json")

# GitHub Actions 환경 최적화 설정 
REPO_MAX_BYTES = int(os.environ.get("REPO_MAX_BYTES", str(1 * 1024**3))) # 1 GiB로 하향
COMMIT_EVERY_FILES = int(os.environ.get("COMMIT_EVERY_FILES", "5000"))
MAX_OPEN_REPOS = 3

WORK_DIR = Path(".work")
WORK_REPOS_DIR = WORK_DIR / "repos"
for d in [WORK_DIR, WORK_REPOS_DIR]: d.mkdir(exist_ok=True)

GH_PAT = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")

# -----------------------------
# Git Utils (Orchestrator Sync)
# -----------------------------
def run(cmd, cwd=None, check=True):
    p = subprocess.run(cmd, cwd=str(cwd) if cwd else None, text=True, capture_output=True)
    if check and p.returncode != 0:
        raise subprocess.CalledProcessError(p.returncode, cmd, output=p.stdout, stderr=p.stderr)
    return p

def sync_orchestrator(msg: str):
    """현재 Orchestrator(본 저장소)의 상태와 통계를 즉시 Push """
    try:
        run(["git", "add", "kalshi_state.json", "KALSHI_REPOS.json", "KALSHI_REPO_STATS.md"], check=False)
        st = run(["git", "status", "--porcelain"])
        if st.stdout.strip():
            run(["git", "config", "user.name", "github-actions[bot]"], check=False)
            run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], check=False)
            run(["git", "commit", "-m", msg])
            run(["git", "push"])
            print(f">>> Orchestrator Checkpoint: {msg}")
    except Exception as e:
        print(f"Orchestrator sync failed: {e}")

# -----------------------------
# Stats Generator (In-process)
# -----------------------------
def update_stats_md(repos_seen):
    """통계 파일을 즉시 생성"""
    lines = [f"# Kalshi Repo Stats", f"Updated: {dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}", ""]
    lines.append("| Repository | Total Files | Note |")
    lines.append("|---|---:|---|")
    for r in sorted(repos_seen):
        lines.append(f"| {r} | - | Active |")
    
    Path("KALSHI_REPO_STATS.md").write_text("\n".join(lines), encoding="utf-8")

# -----------------------------
# Core Classes (Writer & Manager)
# -----------------------------
@dataclass
class RepoCounts:
    updated_utc: str = ""
    total_files: int = 0
    json_files: int = 0
    series_json: int = 0
    event_json: int = 0
    market_json: int = 0

    def to_dict(self):
        return self.__dict__

class RepoWriter:
    def __init__(self, owner, repo, local_path):
        self.owner, self.repo, self.local_path = owner, repo, local_path
        self.counts = RepoCounts()
        self._since_commit = 0

    def ensure_open(self):
        if not (self.local_path / ".git").exists():
            url = f"https://x-access-token:{GH_PAT}@github.com/{self.owner}/{self.repo}.git"
            # 저장소 생성 로직 생략 (기존과 동일하되 내부 호출)
            if self.local_path.exists(): shutil.rmtree(self.local_path)
            run(["git", "clone", "--depth", "1", url, str(self.local_path)])
            run(["git", "config", "user.email", "actions@github.com"], cwd=self.local_path)
            run(["git", "config", "user.name", "github-actions"], cwd=self.local_path)

    def write_json(self, relpath, obj, kind):
        self.ensure_open()
        out = self.local_path / relpath
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(obj, f, separators=(",", ":"))
        self._since_commit += 1
        self.counts.total_files += 1

    def flush(self):
        if self._since_commit == 0: return
        self.ensure_open()
        counts_path = self.local_path / "KALSHI_COUNTS.json"
        with open(counts_path, "w") as f: json.dump(self.counts.to_dict(), f)
        run(["git", "add", "-A"], cwd=self.local_path)
        run(["git", "commit", "-m", f"update data {NOW_UTC}"], cwd=self.local_path)
        run(["git", "push"], cwd=self.local_path)
        self._since_commit = 0

# -----------------------------
# Main Execution
# -----------------------------
def main():
    # 1. Load State
    state = {}
    if STATE_PATH.exists():
        state = json.loads(STATE_PATH.read_text())
    
    state.setdefault("cursors", {})
    state.setdefault("rollover", {})
    
    wm = {} # Simple writer manager
    repos_seen = set(state.get("repos_seen", []))

    def checkpoint(msg):
        """상태 저장 + 통계 갱신 + 푸시 """
        for w in wm.values(): w.flush()
        state["repos_seen"] = list(repos_seen)
        STATE_PATH.write_text(json.dumps(state, indent=2))
        update_stats_md(repos_seen)
        sync_orchestrator(msg)

    try:
        # 예시: Events 크롤링 루프
        # 실제 구현시 paginate 함수와 연동
        for i in range(1, 100000): 
            # (API 호출 및 데이터 생성 로직...)
            repo_name = f"Statground_Data_Kalshi_Events_2026_001" # 예시 가변 이름
            repos_seen.add(repo_name)
            
            if repo_name not in wm:
                wm[repo_name] = RepoWriter(OWNER, repo_name, WORK_REPOS_DIR / repo_name)
            
            # wm[repo_name].write_json(...) 
            
            if i % COMMIT_EVERY_FILES == 0:
                checkpoint(f"kalshi: progress checkpoint at {i}")

    except Exception as e:
        checkpoint(f"kalshi: emergency checkpoint on error")
        raise

if __name__ == "__main__":
    main()