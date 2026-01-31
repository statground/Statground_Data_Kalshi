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
# GitHub 권장 사항 및 퍼포먼스를 고려하여 30,000 ~ 50,000개 사이 권장
REPO_MAX_FILES = 30000 

# 커밋 주기 (파일 개수 기준) - 너무 자주 하면 Git 부하, 너무 적게 하면 메모리 부하
COMMIT_EVERY_FILES = 3000

# 기본 설정
START_TIME = time.time()
NOW_UTC = dt.datetime.now(dt.timezone.utc)
OWNER = os.environ.get("GITHUB_OWNER", "statground").strip()
BASE_URL = os.environ.get("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2").strip()

# GitHub Personal Access Token (Secrets에서 주입됨)
GH_PAT = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")

# 상태 파일 및 작업 경로
STATE_PATH = Path("kalshi_state.json")
WORK_DIR = Path(".work")
WORK_REPOS_DIR = WORK_DIR / "repos"

# 작업 디렉토리 생성
for d in [WORK_DIR, WORK_REPOS_DIR]:
    d.mkdir(exist_ok=True, parents=True)


# ------------------------------------------------------------------------------
# 2. Helper Functions
# ------------------------------------------------------------------------------

def load_state():
    """상태 파일(커서 위치, 리포지토리 번호 등)을 로드합니다."""
    if not STATE_PATH.exists():
        return {"cursors": {}, "rollover": {}, "repos_seen": []}
    try:
        return json.loads(STATE_PATH.read_text(encoding='utf-8'))
    except Exception as e:
        print(f"Warning: Failed to load state ({e}). Starting fresh.", flush=True)
        return {"cursors": {}, "rollover": {}, "repos_seen": []}

def save_state(state):
    """상태 파일을 저장합니다."""
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding='utf-8')

def get_unique_id(kind, data):
    """
    데이터 종류별 고유 식별자(ID)를 추출합니다.
    - Market: ticker (예: KXHV-25DEC-12.5)
    - Event: event_ticker (예: KXHV-25DEC)
    - Series: ticker
    """
    if kind == 'market':
        return data.get('ticker')
    elif kind == 'event':
        return data.get('event_ticker')  # [중요] Event 데이터의 Key 수정
    elif kind == 'series':
        return data.get('ticker')
    return None

def run_git_cmd(cwd, args):
    """특정 디렉토리에서 git 명령어를 실행합니다."""
    cmd = ["git"] + args
    subprocess.run(cmd, cwd=cwd, check=True, capture_output=True)

def setup_repo(repo_name, local_path):
    """
    로컬 폴더를 Git 저장소로 초기화하고 Remote를 연결합니다.
    이미 존재하면 패스합니다.
    """
    if not local_path.exists():
        local_path.mkdir(parents=True)
    
    git_dir = local_path / ".git"
    if not git_dir.exists():
        print(f"Initializing new repo: {repo_name} at {local_path}", flush=True)
        try:
            run_git_cmd(local_path, ["init"])
            # 사용자 설정 (CI 환경용)
            run_git_cmd(local_path, ["config", "user.name", "github-actions[bot]"])
            run_git_cmd(local_path, ["config", "user.email", "github-actions[bot]@users.noreply.github.com"])
            
            # Remote 설정
            remote_url = f"https://x-access-token:{GH_PAT}@github.com/{OWNER}/{repo_name}.git"
            run_git_cmd(local_path, ["remote", "add", "origin", remote_url])
            
            # Pull 시도 (기존 데이터가 있을 수 있음) - 실패해도 무방 (빈 저장소일 수 있음)
            try:
                run_git_cmd(local_path, ["pull", "origin", "main"])
            except:
                pass # 브랜치가 없거나 빈 저장소인 경우
        except Exception as e:
            print(f"Error setting up repo {repo_name}: {e}", flush=True)


# ------------------------------------------------------------------------------
# 3. RepoWriter Class (데이터 저장 및 Git 관리)
# ------------------------------------------------------------------------------

class RepoWriter:
    def __init__(self, repo_name):
        self.repo_name = repo_name
        self.local_path = WORK_REPOS_DIR / repo_name
        self.pending_count = 0
        
        # 저장소 초기화
        setup_repo(repo_name, self.local_path)

    def get_file_count(self):
        """현재 로컬 디렉토리의 JSON 파일 수를 반환합니다."""
        return len(list(self.local_path.glob("*.json")))

    def write_item(self, filename, data):
        """파일을 쓰고 카운트를 증가시킵니다."""
        file_path = self.local_path / filename
        
        # 파일 쓰기 (기존 파일이 있어도 덮어씀 - 최신 상태 반영)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        self.pending_count += 1

    def sync(self):
        """쌓인 변경사항을 커밋하고 푸시합니다."""
        if self.pending_count == 0:
            return

        try:
            print(f"Syncing {self.repo_name} ({self.pending_count} files)...", flush=True)
            run_git_cmd(self.local_path, ["add", "."])
            
            # 변경사항이 있는지 확인
            status = subprocess.run(["git", "status", "--porcelain"], cwd=self.local_path, capture_output=True, text=True)
            if status.stdout.strip():
                timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
                run_git_cmd(self.local_path, ["commit", "-m", f"Data update: {timestamp}"])
                
                # Push (실패 시 재시도 로직은 생략했으나, 실제 운영시엔 필요할 수 있음)
                run_git_cmd(self.local_path, ["push", "-u", "origin", "main"])
                print(f"Successfully pushed {self.repo_name}.", flush=True)
            else:
                print(f"No changes to commit for {self.repo_name}.", flush=True)
            
            self.pending_count = 0 # 카운터 초기화
            
        except Exception as e:
            print(f"Error syncing {self.repo_name}: {e}", flush=True)


# ------------------------------------------------------------------------------
# 4. Main Crawl Logic
# ------------------------------------------------------------------------------

def run_crawl():
    if not GH_PAT:
        print("Error: GH_PAT (GitHub Token) is missing.", flush=True)
        sys.exit(1)

    state = load_state()
    session = requests.Session()
    
    # 관리할 RepoWriter 인스턴스들을 담을 딕셔너리
    # Key: repo_name, Value: RepoWriter Object
    writers = {} 

    # 수집 대상 정의: (kind, API_endpoint, JSON_response_key)
    targets = [
        ("series", "/series", "series"),
        ("event", "/events", "events"),
        ("market", "/markets", "markets")
    ]

    try:
        for kind, endpoint, json_key in targets:
            print(f"--- Starting crawl for: {kind} ---", flush=True)
            
            cursor = state["cursors"].get(kind)
            
            while True:
                # 1. API 호출
                params = {"limit": 100}
                if cursor:
                    params["cursor"] = cursor
                
                try:
                    url = f"{BASE_URL}{endpoint}"
                    resp = session.get(url, params=params, timeout=20)
                    
                    if resp.status_code == 429: # Rate Limit
                        print("Rate limit hit (429). Sleeping 5s...", flush=True)
                        time.sleep(5)
                        continue
                    
                    resp.raise_for_status()
                    data = resp.json()
                    items = data.get(json_key, [])
                    
                except Exception as e:
                    print(f"API Request Failed: {e}", flush=True)
                    time.sleep(10)
                    continue # 재시도

                if not items:
                    print(f"No more items for {kind}.", flush=True)
                    break

                # 2. 아이템 처리 및 저장
                for item in items:
                    uid = get_unique_id(kind, item)
                    if not uid:
                        continue # 식별자 없는 데이터 스킵
                    
                    # 2-1. 리포지토리 이름 결정 (연도별 + 번호별 분산)
                    # 실제 로직에서는 item['open_date'] 등을 파싱해야 하지만, 
                    # 여기서는 간단히 2026년으로 고정하거나 기존 state를 따름
                    target_year = "2026" 
                    
                    prefix = f"Statground_Data_Kalshi_{kind.capitalize()}s_{target_year}"
                    if kind == "series":
                        prefix = "Statground_Data_Kalshi_Series" # Series는 단일 리포지토리 유지
                    
                    # 현재 인덱스 확인 (예: 001, 002...)
                    current_idx = state["rollover"].get(prefix, 1)
                    repo_name = f"{prefix}_{current_idx:03d}"
                    if kind == "series":
                        repo_name = prefix

                    # 2-2. RepoWriter 준비
                    if repo_name not in writers:
                        writers[repo_name] = RepoWriter(repo_name)
                        # repos_seen 업데이트
                        if repo_name not in state["repos_seen"]:
                            state["repos_seen"].append(repo_name)

                    writer = writers[repo_name]

                    # 2-3. Rollover 체크 (파일이 꽉 찼는지)
                    # 주의: Series는 보통 개수가 적으므로 분할하지 않음
                    if kind != "series" and writer.get_file_count() >= REPO_MAX_FILES:
                        # 현재 Writer 동기화(Push) 후 닫기
                        writer.sync()
                        del writers[repo_name]
                        
                        # 인덱스 증가 및 상태 저장
                        current_idx += 1
                        state["rollover"][prefix] = current_idx
                        save_state(state)
                        
                        # 새 리포지토리 이름 설정
                        repo_name = f"{prefix}_{current_idx:03d}"
                        print(f"🔄 [Rollover] Switching to new repo: {repo_name}", flush=True)
                        
                        # 새 Writer 생성 및 등록
                        writers[repo_name] = RepoWriter(repo_name)
                        writer = writers[repo_name]
                        
                        if repo_name not in state["repos_seen"]:
                            state["repos_seen"].append(repo_name)

                    # 2-4. 파일 쓰기
                    file_name = f"{uid}.json"
                    writer.write_item(file_name, item)

                    # 2-5. 중간 커밋 (메모리 보호 및 API 타임아웃 방지)
                    if writer.pending_count >= COMMIT_EVERY_FILES:
                        writer.sync()

                # 3. 커서 업데이트 및 저장
                next_cursor = data.get("cursor")
                if not next_cursor or next_cursor == cursor:
                    state["cursors"][kind] = None # 완료됨
                    save_state(state)
                    break
                
                cursor = next_cursor
                state["cursors"][kind] = cursor
                save_state(state)
                
                # API 부하 조절
                time.sleep(0.1)

    except KeyboardInterrupt:
        print("Crawl interrupted by user.", flush=True)
    except Exception as e:
        print(f"Unexpected Error in run_crawl: {e}", flush=True)
        # 에러가 나더라도 지금까지 작업한 내용은 푸시 시도
    finally:
        # 4. 종료 전 남아있는 변경사항 모두 푸시
        print("Finalizing... Syncing all pending changes.", flush=True)
        for r_name, writer in writers.items():
            writer.sync()
        
        # 통계 파일 업데이트 (선택 사항, 모듈이 있다면 실행)
        try:
            import kalshi_generate_repo_stats_md as stats_gen
            stats_gen.update_stats()
            print("Stats updated.", flush=True)
        except ImportError:
            pass
        except Exception as e:
            print(f"Failed to update stats: {e}", flush=True)

if __name__ == "__main__":
    run_crawl()