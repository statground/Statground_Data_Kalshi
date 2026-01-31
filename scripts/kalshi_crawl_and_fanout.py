#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import subprocess
import datetime as dt
from pathlib import Path
import requests

# 통계 생성 모듈 (없으면 무시)
try:
    import kalshi_generate_repo_stats_md as stats_gen
except ImportError:
    stats_gen = None

# ------------------------------------------------------------------------------
# 1. Configuration & Constants
# ------------------------------------------------------------------------------

# [설정] 리포지토리 자동 분할 기준 (파일 수)
# 사용자의 요청에 따라 100만 개로 상향 조정
REPO_MAX_FILES = 1000000 

# [설정] 커밋 및 통계 갱신 주기 (파일 수)
# 5,000개마다 데이터 Push 및 메인 저장소 통계 반영
COMMIT_EVERY_FILES = 5000

# [설정] 안전 종료 시간 설정 (GitHub Actions 6시간 제한 대비)
JOB_TIME_LIMIT_SEC = 6 * 3600 
FINISH_BUFFER_SEC = 15 * 60 

START_TIME = time.time()
NOW_UTC = dt.datetime.now(dt.timezone.utc)
OWNER = os.environ.get("GITHUB_OWNER", "statground").strip()
BASE_URL = os.environ.get("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2").strip()
GH_PAT = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")

STATE_PATH = Path("kalshi_state.json")
WORK_DIR = Path(".work")
WORK_REPOS_DIR = WORK_DIR / "repos"

for d in [WORK_DIR, WORK_REPOS_DIR]:
    d.mkdir(exist_ok=True, parents=True)


# ------------------------------------------------------------------------------
# 2. GitHub API & Git Helper Functions
# ------------------------------------------------------------------------------

def ensure_remote_repo(repo_name):
    """GitHub 리포지토리가 없으면 자동으로 생성"""
    if not GH_PAT: return

    headers = {
        "Authorization": f"token {GH_PAT}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    if requests.get(f"https://api.github.com/repos/{OWNER}/{repo_name}", headers=headers).status_code == 200:
        return
    
    print(f"⚠️ Repo '{OWNER}/{repo_name}' not found. Creating...", flush=True)
    payload = {"name": repo_name, "private": False}
    res = requests.post(f"https://api.github.com/orgs/{OWNER}/repos", headers=headers, json=payload)
    if res.status_code not in [200, 201]:
        res = requests.post("https://api.github.com/user/repos", headers=headers, json=payload)
    
    if res.status_code in [200, 201]:
        print(f"✅ Created repo: {repo_name}", flush=True)
        time.sleep(3) # 전파 대기

def run_git_cmd(cwd, args):
    subprocess.run(["git"] + args, cwd=cwd, check=True, capture_output=True)

def setup_repo(repo_name, local_path):
    """로컬 Git 초기화 및 동기화"""
    ensure_remote_repo(repo_name)
    if not local_path.exists(): local_path.mkdir(parents=True)
    
    if not (local_path / ".git").exists():
        try:
            run_git_cmd(local_path, ["init"])
            run_git_cmd(local_path, ["config", "user.name", "github-actions[bot]"])
            run_git_cmd(local_path, ["config", "user.email", "github-actions[bot]@users.noreply.github.com"])
            run_git_cmd(local_path, ["branch", "-M", "main"])
            
            remote = f"https://x-access-token:{GH_PAT}@github.com/{OWNER}/{repo_name}.git"
            try: run_git_cmd(local_path, ["remote", "add", "origin", remote])
            except: run_git_cmd(local_path, ["remote", "set-url", "origin", remote])
            
            try: run_git_cmd(local_path, ["pull", "origin", "main"])
            except: pass
        except Exception as e:
            print(f"Repo setup error {repo_name}: {e}", flush=True)

def sync_main_repo(msg_suffix=""):
    """메인 저장소(상태 및 통계) 동기화"""
    try:
        run_git_cmd(Path("."), ["add", "kalshi_state.json", "KALSHI_REPO_STATS.md"])
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
        if status.stdout.strip():
            ts = dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            run_git_cmd(Path("."), ["commit", "-m", f"Update state: {ts} {msg_suffix} [skip ci]"])
            try: run_git_cmd(Path("."), ["pull", "--rebase", "origin", "main"])
            except: pass
            run_git_cmd(Path("."), ["push"])
            print(f" >> 📊 Stats Pushed ({msg_suffix})", flush=True)
    except Exception as e:
        print(f"Main sync failed: {e}", flush=True)


# ------------------------------------------------------------------------------
# 3. Data Logic Helpers
# ------------------------------------------------------------------------------

def load_state():
    if not STATE_PATH.exists():
        return {"cursors": {}, "rollover": {}, "repos_seen": []}
    try: return json.loads(STATE_PATH.read_text(encoding='utf-8'))
    except: return {"cursors": {}, "rollover": {}, "repos_seen": []}

def save_state(state):
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding='utf-8')

def get_unique_id(kind, data):
    if kind == 'market': return data.get('ticker')
    elif kind == 'event': return data.get('event_ticker')
    elif kind == 'series': return data.get('ticker')
    return None

def extract_year(data):
    date_str = data.get('open_date') or data.get('created_time')
    if date_str:
        try: return str(date_str)[:4]
        except: pass
    return str(NOW_UTC.year)


# ------------------------------------------------------------------------------
# 4. RepoWriter Class (샤딩 로직 포함)
# ------------------------------------------------------------------------------

class RepoWriter:
    def __init__(self, repo_name):
        self.repo_name = repo_name
        self.local_path = WORK_REPOS_DIR / repo_name
        self.pending_count = 0
        setup_repo(repo_name, self.local_path)

    def get_file_count(self):
        """디렉토리가 나뉘어 있으므로 재귀적으로 파일 수를 셉니다."""
        return sum(len(files) for _, _, files in os.walk(self.local_path) if '.git' not in _)

    def write_item(self, uid, data):
        """
        [복구된 기능] 디렉토리 샤딩(Sharding) 적용
        UID의 앞 2글자를 디렉토리명으로 사용하여 파일 집중 현상 방지
        """
        filename = f"{uid}.json"
        shard_dir = self.local_path / uid[:2].upper()
        shard_dir.mkdir(exist_ok=True, parents=True)
        
        file_path = shard_dir / filename
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        self.pending_count += 1

    def sync(self):
        if self.pending_count == 0: return
        try:
            print(f"Syncing {self.repo_name}...", flush=True)
            run_git_cmd(self.local_path, ["add", "."])
            status = subprocess.run(["git", "status", "--porcelain"], cwd=self.local_path, capture_output=True, text=True)
            if status.stdout.strip():
                ts = dt.datetime.now(dt.timezone.utc).isoformat()
                run_git_cmd(self.local_path, ["commit", "-m", f"Update data: {ts}"])
                try:
                    run_git_cmd(self.local_path, ["push", "-u", "origin", "main"])
                except:
                    run_git_cmd(self.local_path, ["pull", "--rebase", "origin", "main"])
                    run_git_cmd(self.local_path, ["push", "-u", "origin", "main"])
            self.pending_count = 0
        except Exception as e:
            print(f"Sync error {self.repo_name}: {e}", flush=True)


# ------------------------------------------------------------------------------
# 5. Main Execution
# ------------------------------------------------------------------------------

def run_crawl():
    if not GH_PAT:
        print("Error: GH_PAT missing.", flush=True)
        sys.exit(1)

    state = load_state()
    session = requests.Session()
    writers = {} 

    targets = [
        ("series", "/series", "series"),
        ("event", "/events", "events"),
        ("market", "/markets", "markets")
    ]

    try:
        for kind, endpoint, json_key in targets:
            print(f"--- Crawling {kind} ---", flush=True)
            cursor = state["cursors"].get(kind)
            
            while True:
                # 안전 종료 체크
                if (time.time() - START_TIME) > (JOB_TIME_LIMIT_SEC - FINISH_BUFFER_SEC):
                    print("⏳ Time limit. Graceful stop.", flush=True)
                    return

                params = {"limit": 100}
                if cursor: params["cursor"] = cursor
                
                try:
                    resp = session.get(f"{BASE_URL}{endpoint}", params=params, timeout=20)
                    if resp.status_code == 429:
                        time.sleep(10)
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                    items = data.get(json_key, [])
                except Exception as e:
                    print(f"API Error: {e}", flush=True)
                    time.sleep(10)
                    continue

                if not items: break

                for item in items:
                    uid = get_unique_id(kind, item)
                    if not uid: continue
                    
                    target_year = extract_year(item)
                    prefix = f"Statground_Data_Kalshi_{kind.capitalize()}s_{target_year}"
                    if kind == "series": prefix = "Statground_Data_Kalshi_Series"
                    
                    current_idx = state["rollover"].get(prefix, 1)
                    repo_name = f"{prefix}_{current_idx:03d}"
                    if kind == "series": repo_name = prefix

                    if repo_name not in writers:
                        writers[repo_name] = RepoWriter(repo_name)
                        if repo_name not in state["repos_seen"]:
                            state["repos_seen"].append(repo_name)

                    writer = writers[repo_name]

                    # Rollover 체크 (100만 개 기준)
                    if kind != "series" and writer.get_file_count() >= REPO_MAX_FILES:
                        print(f"🔄 Rolling over {repo_name} (Limit {REPO_MAX_FILES})", flush=True)
                        writer.sync()
                        del writers[repo_name]
                        
                        current_idx += 1
                        state["rollover"][prefix] = current_idx
                        save_state(state)
                        
                        repo_name = f"{prefix}_{current_idx:03d}"
                        writers[repo_name] = RepoWriter(repo_name)
                        writer = writers[repo_name]
                        if repo_name not in state["repos_seen"]:
                            state["repos_seen"].append(repo_name)

                    # 데이터 저장 (UID 전달)
                    writer.write_item(uid, item)

                    # 5,000개 마다 커밋 및 통계 갱신
                    if writer.pending_count >= COMMIT_EVERY_FILES:
                        writer.sync()
                        if stats_gen: 
                            try: stats_gen.update_stats()
                            except: pass
                        save_state(state)
                        sync_main_repo(f"{kind} {current_idx:03d}")

                next_cursor = data.get("cursor")
                if not next_cursor or next_cursor == cursor:
                    state["cursors"][kind] = None
                    save_state(state)
                    break
                
                cursor = next_cursor
                state["cursors"][kind] = cursor
                save_state(state)
                time.sleep(0.1)

    except Exception as e:
        print(f"Unexpected Error: {e}", flush=True)
    finally:
        print("Finalizing...", flush=True)
        for w in writers.values():
            w.sync()
        if stats_gen: 
            try: stats_gen.update_stats()
            except: pass
        save_state(state)
        sync_main_repo("Finished")

if __name__ == "__main__":
    run_crawl()