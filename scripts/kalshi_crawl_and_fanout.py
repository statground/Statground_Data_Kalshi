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

# ------------------------------------------------------------------------------
# 1. Configuration & Constants
# ------------------------------------------------------------------------------

# 리포지토리 자동 분할 기준 (파일 수)
REPO_MAX_FILES = 30000 
COMMIT_EVERY_FILES = 3000

# 기본 설정
START_TIME = time.time()
OWNER = os.environ.get("GITHUB_OWNER", "statground").strip()
BASE_URL = os.environ.get("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2").strip()

# GitHub Personal Access Token
GH_PAT = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")

# 상태 파일 및 작업 경로
STATE_PATH = Path("kalshi_state.json")
WORK_DIR = Path(".work")
WORK_REPOS_DIR = WORK_DIR / "repos"

for d in [WORK_DIR, WORK_REPOS_DIR]:
    d.mkdir(exist_ok=True, parents=True)


# ------------------------------------------------------------------------------
# 2. GitHub API Helper (Auto-Create Repo)
# ------------------------------------------------------------------------------

def ensure_remote_repo(repo_name):
    """
    GitHub에 리포지토리가 존재하는지 확인하고, 없으면 API로 생성합니다.
    (삭제된 저장소 자동 복구 기능)
    """
    if not GH_PAT:
        print("Warning: GH_PAT not found. Skipping remote repo check.", flush=True)
        return

    headers = {
        "Authorization": f"token {GH_PAT}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    # 1. 존재 여부 확인
    chk_url = f"https://api.github.com/repos/{OWNER}/{repo_name}"
    resp = requests.get(chk_url, headers=headers)
    
    if resp.status_code == 200:
        # 이미 존재함
        return
    
    print(f"⚠️ Repo '{OWNER}/{repo_name}' not found. Creating...", flush=True)
    
    # 2. 생성 시도 (먼저 Organization 하위 생성 시도)
    payload = {
        "name": repo_name,
        "private": False,  # Public 저장소로 생성 (필요 시 True로 변경)
        "has_issues": False,
        "has_projects": False,
        "has_wiki": False
    }
    
    # Org에 생성 시도
    create_url = f"https://api.github.com/orgs/{OWNER}/repos"
    create_resp = requests.post(create_url, headers=headers, json=payload)
    
    # Org 생성이 권한 문제 등으로 실패하면, 개인 계정(User)에 생성 시도
    if create_resp.status_code not in [200, 201]:
        print(f"  -> Failed to create in Org '{OWNER}' ({create_resp.status_code}). Trying User scope...", flush=True)
        create_url = "https://api.github.com/user/repos"
        create_resp = requests.post(create_url, headers=headers, json=payload)
    
    if create_resp.status_code in [200, 201]:
        print(f"✅ Successfully created repo: {repo_name}", flush=True)
        time.sleep(2) # GitHub 전파 대기
    else:
        print(f"❌ Failed to create repo: {create_resp.text}", flush=True)
        # 여기서 죽지 않고 로컬에라도 쌓도록 진행


# ------------------------------------------------------------------------------
# 3. Helper Functions
# ------------------------------------------------------------------------------

def load_state():
    """상태 파일이 없으면 초기값으로 생성"""
    if not STATE_PATH.exists():
        return {"cursors": {}, "rollover": {}, "repos_seen": []}
    try:
        return json.loads(STATE_PATH.read_text(encoding='utf-8'))
    except Exception as e:
        print(f"Warning: Failed to load state ({e}). Starting fresh.", flush=True)
        return {"cursors": {}, "rollover": {}, "repos_seen": []}

def save_state(state):
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding='utf-8')

def get_unique_id(kind, data):
    if kind == 'market':
        return data.get('ticker')
    elif kind == 'event':
        return data.get('event_ticker') 
    elif kind == 'series':
        return data.get('ticker')
    return None

def run_git_cmd(cwd, args):
    cmd = ["git"] + args
    subprocess.run(cmd, cwd=cwd, check=True, capture_output=True)

def setup_repo(repo_name, local_path):
    """로컬 Git 초기화 및 Remote 연결 (안전장치 포함)"""
    
    # [핵심] 원격 저장소가 없으면 만든다.
    ensure_remote_repo(repo_name)

    if not local_path.exists():
        local_path.mkdir(parents=True)
    
    git_dir = local_path / ".git"
    if not git_dir.exists():
        print(f"Initializing local repo: {repo_name}", flush=True)
        try:
            run_git_cmd(local_path, ["init"])
            run_git_cmd(local_path, ["config", "user.name", "github-actions[bot]"])
            run_git_cmd(local_path, ["config", "user.email", "github-actions[bot]@users.noreply.github.com"])
            # 기본 브랜치 main 강제
            run_git_cmd(local_path, ["branch", "-M", "main"])
            
            remote_url = f"https://x-access-token:{GH_PAT}@github.com/{OWNER}/{repo_name}.git"
            try:
                run_git_cmd(local_path, ["remote", "add", "origin", remote_url])
            except:
                # 이미 있으면 url 변경
                run_git_cmd(local_path, ["remote", "set-url", "origin", remote_url])
            
            # Pull 시도 (데이터가 있으면 가져오고, 없으면 패스)
            try:
                run_git_cmd(local_path, ["pull", "origin", "main"])
            except:
                # 빈 저장소일 경우 pull 실패는 자연스러운 현상
                pass
        except Exception as e:
            print(f"Error setting up repo {repo_name}: {e}", flush=True)


# ------------------------------------------------------------------------------
# 4. RepoWriter Class
# ------------------------------------------------------------------------------

class RepoWriter:
    def __init__(self, repo_name):
        self.repo_name = repo_name
        self.local_path = WORK_REPOS_DIR / repo_name
        self.pending_count = 0
        
        setup_repo(repo_name, self.local_path)

    def get_file_count(self):
        return len(list(self.local_path.glob("*.json")))

    def write_item(self, filename, data):
        file_path = self.local_path / filename
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        self.pending_count += 1

    def sync(self):
        if self.pending_count == 0:
            return

        try:
            print(f"Syncing {self.repo_name} ({self.pending_count} changes)...", flush=True)
            run_git_cmd(self.local_path, ["add", "."])
            
            status = subprocess.run(["git", "status", "--porcelain"], cwd=self.local_path, capture_output=True, text=True)
            if status.stdout.strip():
                timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
                run_git_cmd(self.local_path, ["commit", "-m", f"Data update: {timestamp}"])
                
                # Push 시도
                try:
                    run_git_cmd(self.local_path, ["push", "-u", "origin", "main"])
                    print(f"-> Pushed {self.repo_name}", flush=True)
                except Exception as e:
                    # Push 실패 시 Pull Rebase 후 재시도 (동시성 문제 해결)
                    print(f"Push failed, retrying with pull --rebase... ({e})", flush=True)
                    run_git_cmd(self.local_path, ["pull", "--rebase", "origin", "main"])
                    run_git_cmd(self.local_path, ["push", "-u", "origin", "main"])
            
            self.pending_count = 0
            
        except Exception as e:
            print(f"Error syncing {self.repo_name}: {e}", flush=True)


# ------------------------------------------------------------------------------
# 5. Main Crawl Logic
# ------------------------------------------------------------------------------

def run_crawl():
    if not GH_PAT:
        print("Error: GH_PAT is missing. Cannot interact with GitHub.", flush=True)
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
            
            # API 호출이 없어도 루프가 한 번은 돌아야 상태가 저장됨.
            # 하지만 여기서는 커서 기반 페이지네이션이므로 While문 진입
            
            while True:
                params = {"limit": 100}
                if cursor: params["cursor"] = cursor
                
                try:
                    url = f"{BASE_URL}{endpoint}"
                    resp = session.get(url, params=params, timeout=20)
                    if resp.status_code == 429:
                        time.sleep(5)
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                    items = data.get(json_key, [])
                except Exception as e:
                    print(f"API Error ({kind}): {e}", flush=True)
                    time.sleep(10)
                    continue

                if not items:
                    print(f"No items for {kind}.", flush=True)
                    # 완료 처리: 커서를 null로 만들면 다음 실행 시 처음부터 다시 함(원치 않으면 유지)
                    # 여기서는 그냥 break. 만약 '완료됨'을 표시하려면 별도 플래그 필요
                    # Kalshi API 특성상 '더 이상 없음'이 끝이 아닐 수 있음(실시간 추가)
                    # 따라서 커서를 유지하는 게 맞음.
                    break

                for item in items:
                    uid = get_unique_id(kind, item)
                    if not uid: continue
                    
                    target_year = "2026" # 날짜 파싱 로직 추가 가능
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

                    # Rollover
                    if kind != "series" and writer.get_file_count() >= REPO_MAX_FILES:
                        writer.sync()
                        del writers[repo_name]
                        
                        current_idx += 1
                        state["rollover"][prefix] = current_idx
                        save_state(state)
                        
                        repo_name = f"{prefix}_{current_idx:03d}"
                        print(f"🔄 Rolling over to: {repo_name}", flush=True)
                        
                        writers[repo_name] = RepoWriter(repo_name)
                        writer = writers[repo_name]
                        if repo_name not in state["repos_seen"]:
                            state["repos_seen"].append(repo_name)

                    writer.write_item(f"{uid}.json", item)

                    if writer.pending_count >= COMMIT_EVERY_FILES:
                        writer.sync()

                next_cursor = data.get("cursor")
                if not next_cursor or next_cursor == cursor:
                    state["cursors"][kind] = None # End of pagination for now
                    save_state(state)
                    break
                
                cursor = next_cursor
                state["cursors"][kind] = cursor
                save_state(state)
                time.sleep(0.1)

    except KeyboardInterrupt:
        print("Interrupted.", flush=True)
    except Exception as e:
        print(f"Unexpected Error: {e}", flush=True)
    finally:
        print("Finalizing...", flush=True)
        for w in writers.values():
            w.sync()
        
        # 통계 갱신 시도
        try:
            import kalshi_generate_repo_stats_md as stats_gen
            stats_gen.update_stats()
        except:
            pass

if __name__ == "__main__":
    run_crawl()