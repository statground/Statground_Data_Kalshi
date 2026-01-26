#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kalshi (elections) crawler + GitHub fan-out.

Designed for GitHub Actions runners (limited disk).

Features
- Crawl: /series, /events, /markets
- Write each object as one JSON file to GitHub repos.
- Repo fan-out:
    * Series:  Statground_Data_Kalshi_Series
    * Events:  Statground_Data_Kalshi_Events_<YYYY>_<NNN> (preferred if year known)
              Statground_Data_Kalshi_Events_Current_<NNN> (fallback)
    * Markets: Statground_Data_Kalshi_Markets_<YYYY>_<NNN> (preferred if year known)
              Statground_Data_Kalshi_Markets_Current_<NNN> (fallback)
- Disk-based rollover:
    * If disk free < DISK_FREE_MIN_BYTES: flush/push, close, and roll to next repo index when needed
    * If repo local dir size > REPO_MAX_BYTES: roll to next repo index for that prefix
- To keep disk low, when a repo is closed we delete its local clone directory.

Environment variables (GitHub Actions)
- GH_PAT (required): GitHub token with repo scopes
- GITHUB_OWNER (optional, default "statground")
- KALSHI_BASE_URL (optional)
- COMMIT_EVERY_FILES (optional, default 5000)
- MAX_OPEN_REPOS (optional, default 3)
- REPO_MAX_BYTES (optional, default 6 GiB)
- DISK_FREE_MIN_BYTES (optional, default 3 GiB)
- ROLLOVER_SUFFIX_WIDTH (optional, default 3)
- VERBOSE_GIT (optional "1" to show git stdout)
"""

from __future__ import annotations

import os
import sys
import json
import time
import math
import shutil
import hashlib
import logging
import subprocess
import datetime as dt

# -------------------------
# Schedule-aware time budget (KST 00/06/12/18 by default)
# -------------------------
def _parse_int_list(csv: str, default: list[int]) -> list[int]:
    try:
        out = [int(x.strip()) for x in (csv or "").split(",") if x.strip() != ""]
        return out or default
    except Exception:
        return default

def _next_kst_schedule_dt(now_utc: dt.datetime, kst_hours: list[int]) -> dt.datetime:
    """Return next scheduled datetime in Asia/Seoul, converted to UTC."""
    from zoneinfo import ZoneInfo
    kst = ZoneInfo("Asia/Seoul")
    now_kst = now_utc.astimezone(kst)
    hours_sorted = sorted(set(kst_hours))
    # candidate times today
    for h in hours_sorted:
        cand = now_kst.replace(hour=h, minute=0, second=0, microsecond=0)
        if cand > now_kst:
            return cand.astimezone(dt.timezone.utc)
    # otherwise first slot tomorrow
    tomorrow = (now_kst + dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    cand = tomorrow.replace(hour=hours_sorted[0], minute=0, second=0, microsecond=0)
    return cand.astimezone(dt.timezone.utc)

def _compute_effective_deadline(start_utc: dt.datetime, base_budget_sec: int, finish_before_min: int, kst_hours: list[int]) -> float:
    """Return epoch seconds when we should stop (min of base budget and next schedule minus buffer)."""
    now_utc = dt.datetime.now(dt.timezone.utc)
    next_sched_utc = _next_kst_schedule_dt(now_utc, kst_hours)
    buffer_sec = max(0, int(finish_before_min) * 60)
    finish_by_utc = next_sched_utc - dt.timedelta(seconds=buffer_sec)
    base_deadline = start_utc + dt.timedelta(seconds=base_budget_sec)
    effective = min(base_deadline, finish_by_utc)
    return effective.timestamp()
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

# -----------------------------
# Globals / Config
# -----------------------------

NOW_UTC = dt.datetime.now(dt.timezone.utc)
OWNER = os.environ.get("GITHUB_OWNER", "statground").strip()
BASE_URL = os.environ.get("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2").strip()

# The control repo is the repository where this crawler lives (Statground_Data_Kalshi).
# We commit/push small state files (e.g., KALSHI_STATE.json) to it.
_gh_repo = (os.environ.get("GITHUB_REPOSITORY") or "").strip()  # "owner/repo"
if "/" in _gh_repo:
    CONTROL_OWNER, CONTROL_REPO = _gh_repo.split("/", 1)
else:
    CONTROL_OWNER, CONTROL_REPO = OWNER, "Statground_Data_Kalshi"

GH_PAT = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")  # allow either
if not GH_PAT:
    raise RuntimeError("GH_PAT (or GITHUB_TOKEN) is required")

COMMIT_EVERY_FILES = int(os.environ.get("COMMIT_EVERY_FILES", "5000"))
MAX_OPEN_REPOS = int(os.environ.get("MAX_OPEN_REPOS", "3"))
VERBOSE_GIT = os.environ.get("VERBOSE_GIT", "0").strip() == "1"

REPO_MAX_BYTES = int(os.environ.get("REPO_MAX_BYTES", str(6 * 1024**3)))          # 6 GiB
DISK_FREE_MIN_BYTES = int(os.environ.get("DISK_FREE_MIN_BYTES", str(3 * 1024**3)))# 3 GiB
ROLLOVER_SUFFIX_WIDTH = int(os.environ.get("ROLLOVER_SUFFIX_WIDTH", "3"))

WORK_DIR = Path(".work")
WORK_DIR.mkdir(exist_ok=True)
LOG_DIR = WORK_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "kalshi_run.log"

WORK_REPOS_DIR = WORK_DIR / "repos"
WORK_REPOS_DIR.mkdir(exist_ok=True)

STATE_PATH = WORK_DIR / "kalshi_state.json"

UA = "statground-kalshi-crawler/1.0"

# -----------------------------
# Logging
# -----------------------------

def get_logger() -> logging.Logger:
    log = logging.getLogger("kalshi")
    if log.handlers:
        return log

    log.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    # console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    log.addHandler(ch)
    # file
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    log.addHandler(fh)
    return log


log = get_logger()

# -----------------------------
# Utilities
# -----------------------------

def disk_free_bytes(path: Path) -> int:
    usage = shutil.disk_usage(str(path))
    return usage.free

def dir_size_bytes(path: Path, stop_at: Optional[int] = None) -> int:
    """Compute directory size. If stop_at is set, stop once size exceeds it (best-effort)."""
    total = 0
    if not path.exists():
        return 0
    # iterative walk, fast-ish
    for root, dirs, files in os.walk(path):
        for fn in files:
            try:
                fp = Path(root) / fn
                total += fp.stat().st_size
                if stop_at is not None and total > stop_at:
                    return total
            except FileNotFoundError:
                continue
    return total

def sha1_short(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:10]

def ensure_parent(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)

def json_dump(path: Path, obj: Any) -> None:
    ensure_parent(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=False), encoding="utf-8")
    tmp.replace(path)

def safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        if isinstance(x, bool):
            return int(x)
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None

def parse_dt_from_any(obj: Dict[str, Any]) -> Optional[dt.datetime]:
    """Try multiple candidate fields to infer created datetime."""
    candidates = [
        "created_time", "createdTime", "created_ts", "created_at",
        "open_time", "openTime", "last_updated_time", "updated_time",
        "close_time", "closeTime",
    ]
    for k in candidates:
        if k not in obj:
            continue
        v = obj.get(k)
        if v is None:
            continue

        # unix seconds?
        n = safe_int(v)
        if n is not None:
            # heuristics: milliseconds?
            if n > 10_000_000_000:  # ms
                n = n // 1000
            if 0 < n < 4_000_000_000:
                try:
                    return dt.datetime.fromtimestamp(n, tz=dt.timezone.utc)
                except Exception:
                    pass

        # ISO string?
        if isinstance(v, str):
            s = v.strip()
            # allow trailing Z
            try:
                if s.endswith("Z"):
                    s2 = s[:-1] + "+00:00"
                else:
                    s2 = s
                return dt.datetime.fromisoformat(s2)
            except Exception:
                pass
    return None

def yyyymm_from_obj(obj: Dict[str, Any]) -> Tuple[str, str]:
    d = parse_dt_from_any(obj)
    if not d:
        return ("unknown", "unknown")
    return (f"{d.year:04d}", f"{d.month:02d}")

def normalize_rel(rel: Any) -> str:
    # rel may come as Path or str
    if isinstance(rel, Path):
        rel = rel.as_posix()
    rel = str(rel)
    rel = rel.lstrip("/")
    return rel

# -----------------------------
# GitHub API helpers
# -----------------------------

GH_API = "https://api.github.com"

def gh_headers() -> Dict[str, str]:
    return {
        "Authorization": f"token {GH_PAT}",
        "Accept": "application/vnd.github+json",
        "User-Agent": UA,
    }

def gh_repo_exists(owner: str, repo: str) -> bool:
    r = requests.get(f"{GH_API}/repos/{owner}/{repo}", headers=gh_headers(), timeout=30)
    if r.status_code == 403 and "rate limit" in (r.text or "").lower():
        reset = r.headers.get("X-RateLimit-Reset")
        reset_dt = ""
        if reset and str(reset).isdigit():
            try:
                reset_dt = datetime.fromtimestamp(int(reset), timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            except Exception:
                reset_dt = ""
        extra = f" (reset: {reset_dt})" if reset_dt else ""
        raise RuntimeError(f"GitHub API rate limit exceeded while checking repo existence{extra}.")
    return r.status_code == 200

def gh_create_repo(owner: str, repo: str, description: str = "") -> None:
    """
    Create repo under authenticated user account. If owner != authenticated user, this may fail.
    We treat 422 as non-fatal (repo may already exist or name conflict).
    """
    payload = {"name": repo, "description": description, "private": False, "has_issues": False, "has_projects": False, "has_wiki": False}
    r = requests.post(f"{GH_API}/user/repos", headers=gh_headers(), json=payload, timeout=60)
    if r.status_code == 403 and "rate limit" in (r.text or "").lower():
        reset = r.headers.get("X-RateLimit-Reset")
        reset_dt = ""
        if reset and str(reset).isdigit():
            try:
                reset_dt = datetime.fromtimestamp(int(reset), timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            except Exception:
                reset_dt = ""
        extra = f" (reset: {reset_dt})" if reset_dt else ""
        raise RuntimeError(f"GitHub API rate limit exceeded while creating repo {owner}/{repo}{extra}.")
    if r.status_code in (201,):
        log.info("Created repo: %s/%s", owner, repo)
        return
    if r.status_code == 422:
        # Often means \"name already exists\" (or invalid). We'll proceed and let clone decide.
        log.warning("GitHub repo create returned 422 for %s/%s: %s", owner, repo, r.text[:200])
        return
    raise RuntimeError(f"GitHub: repo create failed {owner}/{repo}: {r.status_code} {r.text[:500]}")

def ensure_repo(owner: str, repo: str, description: str = "") -> None:
    # Avoid GitHub REST API calls for existence checks (rate-limit prone).
    # Use `git ls-remote` to test repo reachability; only fall back to REST
    # when we actually need to CREATE the repository.
    # First try without token. If repos are public, this avoids needless auth
    # issues and never touches the REST API rate limit.
    url_public = f"https://github.com/{owner}/{repo}.git"
    p = run(["git", "ls-remote", "-q", url_public], check=False)
    if p.returncode != 0:
        token = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")
        if token:
            url_auth = f"https://x-access-token:{token}@github.com/{owner}/{repo}.git"
            p = run(["git", "ls-remote", "-q", url_auth], check=False)
    if p.returncode == 0:
        return
    # If ls-remote failed for a reason other than non-existence, surface it.
    # Typical non-existence: "Repository not found".
    err = (p.stderr or "").lower()
    # If authentication failed, don't try to create; bubble up clearly.
    if "authentication" in err or "403" in err:
        raise RuntimeError(f"git ls-remote auth failed for {owner}/{repo}: {p.stderr[-500:]}")
    if "not found" not in err and "repository" not in err:
        raise RuntimeError(
            f"git ls-remote failed for {url_public}: {p.stderr[-500:] if p.stderr else p.stdout[-500:]}"
        )

    log.info("Creating missing repo: %s/%s", owner, repo)
    gh_create_repo(owner, repo, description)

# -----------------------------
# Git / Repo writer
# -----------------------------

def run(cmd: List[str], cwd: Optional[Path] = None, check: bool = True) -> subprocess.CompletedProcess:
    if VERBOSE_GIT or cmd[0] != "git":
        log.debug("$ %s", " ".join(cmd))
    p = subprocess.run(cmd, cwd=str(cwd) if cwd else None, text=True, capture_output=True)
    if check and p.returncode != 0:
        # Emit a helpful tail for Actions logs (git often returns 128 with the real reason in stderr).
        stderr_tail = (p.stderr or "").strip()[-1500:]
        stdout_tail = (p.stdout or "").strip()[-1500:]
        if stderr_tail:
            log.error("stderr (tail): %s", stderr_tail)
        if stdout_tail and VERBOSE_GIT:
            log.error("stdout (tail): %s", stdout_tail)
        raise subprocess.CalledProcessError(p.returncode, cmd, output=p.stdout, stderr=p.stderr)
    return p


def _token() -> Optional[str]:
    """Return a PAT used for GitHub API + git push. Prefer GH_PAT."""
    return os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")


def git_set_origin_with_token(repo_path: Path, owner: str, repo: str) -> None:
    """Ensure origin URL includes token so `git push` works in GitHub Actions."""
    tok = _token()
    if not tok:
        return
    # Using x-access-token is the recommended format for HTTPS auth with PAT.
    url = f"https://x-access-token:{tok}@github.com/{owner}/{repo}.git"
    # Avoid logging token.
    try:
        run(["git", "remote", "set-url", "origin", url], cwd=repo_path, check=True)
    except Exception:
        # If remote doesn't exist yet (rare), add it.
        run(["git", "remote", "add", "origin", url], cwd=repo_path, check=True)

def remove_stale_git_locks(repo_path: Path) -> None:
    lock = repo_path / ".git" / "index.lock"
    if lock.exists():
        try:
            lock.unlink()
        except Exception:
            pass

def git_setup_identity(repo_path: Path) -> None:
    # set per-repo identity to avoid failure
    run(["git", "config", "user.email", "actions@github.com"], cwd=repo_path, check=False)
    run(["git", "config", "user.name", "github-actions"], cwd=repo_path, check=False)

def get_github_pat() -> Optional[str]:
    """Return a GitHub token that has permission to push/create repos.

    In this project we expect the user to store `GH_PAT` in GitHub Actions
    Secrets and expose it as env var.
    """
    return os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")

def remote_url(owner: str, repo: str, with_token: bool) -> str:
    if with_token:
        tok = get_github_pat()
        if tok:
            # Use x-access-token style to avoid username requirements.
            return f"https://x-access-token:{tok}@github.com/{owner}/{repo}.git"
    return f"https://github.com/{owner}/{repo}.git"

def git_set_origin_url(repo_path: Path, owner: str, repo: str) -> None:
    """Ensure origin uses an authenticated URL so `git push` works in Actions."""
    url = remote_url(owner, repo, with_token=True)
    # If no token, leave as-is.
    if "x-access-token" not in url:
        return
    # set-url can fail if origin is missing; handle both.
    p = run(["git", "remote", "set-url", "origin", url], cwd=repo_path, check=False)
    if p.returncode != 0:
        run(["git", "remote", "add", "origin", url], cwd=repo_path, check=False)

def git_clone_or_init(owner: str, repo: str, local_path: Path) -> None:
    # Clone with token so the repo is immediately pushable.
    url = remote_url(owner, repo, with_token=True)
    if local_path.exists():
        return
    local_path.parent.mkdir(parents=True, exist_ok=True)
    # shallow clone to keep disk low
    run(["git", "clone", "--depth", "1", url, str(local_path)], check=True)
    git_setup_identity(local_path)
    git_set_origin_url(local_path, owner, repo)

def git_has_changes(repo_path: Path) -> bool:
    remove_stale_git_locks(repo_path)
    p = run(["git", "status", "--porcelain"], cwd=repo_path, check=True)
    return bool(p.stdout.strip())

def git_commit_push_if_changed(repo_path: Path, msg: str, owner: str, repo: str) -> bool:
    remove_stale_git_locks(repo_path)
    if not git_has_changes(repo_path):
        return False
    run(["git", "add", "-A"], cwd=repo_path, check=True)
    # commit may fail if nothing staged (race)
    p = run(["git", "commit", "--quiet", "-m", msg], cwd=repo_path, check=False)
    if p.returncode != 0:
        # re-check if changes vanished
        if not git_has_changes(repo_path):
            return False
        raise subprocess.CalledProcessError(p.returncode, ["git", "commit"], output=p.stdout, stderr=p.stderr)
    # Ensure authenticated origin for push (important on GitHub Actions).
    git_set_origin_url(repo_path, owner, repo)
    # push
    run(["git", "push", "--quiet"], cwd=repo_path, check=True)
    return True

@dataclass
class RepoCounts:
    updated_utc: str = ""
    total_files: int = 0
    json_files: int = 0
    series_json: int = 0
    event_json: int = 0
    market_json: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "updated_utc": self.updated_utc,
            "total_files": self.total_files,
            "json_files": self.json_files,
            "series_json": self.series_json,
            "event_json": self.event_json,
            "market_json": self.market_json,
        }

@dataclass
class RepoWriter:
    owner: str
    repo: str
    description: str
    local_path: Path
    commit_every_files: int
    counts: RepoCounts = field(default_factory=RepoCounts)
    _since_commit: int = 0
    _last_used_ts: float = field(default_factory=lambda: time.time())

    def touch(self) -> None:
        self._last_used_ts = time.time()

    def ensure_open(self) -> None:
        # IMPORTANT: do *not* call GitHub REST API (repo exists / create) on every write.
        # This crawler can write millions of files; an API call per file will quickly
        # hit rate limits. We only ensure the repo + clone once per local worktree.
        if self.local_path.exists() and (self.local_path / ".git").exists():
            self.touch()
            return
        ensure_repo(self.owner, self.repo, self.description)
        git_clone_or_init(self.owner, self.repo, self.local_path)
        self.touch()

    def write_json(self, relpath: str, obj: Dict[str, Any], kind: str) -> None:
        self.ensure_open()
        relpath = normalize_rel(relpath)
        out = self.local_path / relpath
        json_dump(out, obj)
        self._since_commit += 1
        # update counts
        self.counts.total_files += 1
        self.counts.json_files += 1
        if kind == "series":
            self.counts.series_json += 1
        elif kind == "event":
            self.counts.event_json += 1
        elif kind == "market":
            self.counts.market_json += 1

    def maybe_flush(self, force: bool = False) -> None:
        self.touch()
        # update counts file every flush, so stats generator can read it remotely
        if force or self._since_commit >= self.commit_every_files:
            self.flush()

    def flush(self) -> None:
        self.ensure_open()
        self.counts.updated_utc = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        counts_path = self.local_path / "KALSHI_COUNTS.json"
        json_dump(counts_path, self.counts.to_dict())
        msg = f"kalshi: update data ({NOW_UTC.strftime('%Y-%m-%d %H:%M UTC')})"
        git_commit_push_if_changed(self.local_path, msg, self.owner, self.repo)
        self._since_commit = 0

    def close_and_delete(self) -> None:
        # best-effort flush then delete local repo dir
        try:
            self.flush()
        except Exception as e:
            log.warning("flush failed on close for %s: %s", self.repo, e)
        try:
            if self.local_path.exists():
                shutil.rmtree(self.local_path, ignore_errors=True)
        except Exception:
            pass

class WriterManager:
    def __init__(self, owner: str, commit_every_files: int, max_open_repos: int):
        self.owner = owner
        self.commit_every_files = commit_every_files
        self.max_open_repos = max_open_repos
        self._writers: Dict[str, RepoWriter] = {}

    def get(self, repo: str, description: str) -> RepoWriter:
        w = self._writers.get(repo)
        if w is None:
            local_path = WORK_REPOS_DIR / repo
            w = RepoWriter(self.owner, repo, description, local_path, self.commit_every_files)
            self._writers[repo] = w
        w.touch()
        self._enforce_open_limit()
        return w

    def _enforce_open_limit(self) -> None:
        # We keep writer objects, but we want to keep at most N local clones on disk.
        # Close (flush + delete) least recently used clones if too many exist.
        open_local = [(r, w) for r, w in self._writers.items() if w.local_path.exists()]
        if len(open_local) <= self.max_open_repos:
            return
        open_local.sort(key=lambda rw: rw[1]._last_used_ts)
        while len(open_local) > self.max_open_repos:
            repo, w = open_local.pop(0)
            log.info("Closing repo to save disk (LRU): %s", repo)
            w.close_and_delete()

    def flush_all(self) -> None:
        for w in list(self._writers.values()):
            try:
                w.flush()
            except Exception as e:
                log.warning("flush_all failed for %s: %s", w.repo, e)

    def close_all(self) -> None:
        for w in list(self._writers.values()):
            w.close_and_delete()

# -----------------------------
# Rollover tracker
# -----------------------------

class RepoRolloverTracker:
    """
    Tracks current index (NNN) for each repo prefix (e.g. Statground_Data_Kalshi_Events_2026).
    The index increments when:
      - disk is low OR
      - repo local size exceeds REPO_MAX_BYTES
    """

    def __init__(self, state: Dict[str, Any]):
        self.state = state
        self.state.setdefault("rollover", {})  # prefix -> index int

    def current_index(self, prefix: str) -> int:
        return int(self.state["rollover"].get(prefix, 1))

    def bump(self, prefix: str) -> int:
        cur = self.current_index(prefix)
        nxt = cur + 1
        self.state["rollover"][prefix] = nxt
        return nxt

    def repo_name(self, prefix: str) -> str:
        idx = self.current_index(prefix)
        return f"{prefix}_{idx:0{ROLLOVER_SUFFIX_WIDTH}d}"

# -----------------------------
# API crawling
# -----------------------------

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})

def api_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = BASE_URL.rstrip("/") + path
    r = SESSION.get(url, params=params or {}, timeout=60)
    r.raise_for_status()
    return r.json()

def paginate(path: str, list_key: str, params: Optional[Dict[str, Any]] = None, cursor_key: str = "cursor") -> Iterable[Dict[str, Any]]:
    """
    Generic cursor pagination.
    Kalshi trade-api/v2 typically returns:
      { <list_key>: [...], cursor: \"...\" }  OR { <list_key>: [...], next_cursor: \"...\" }
    """
    cursor = None
    p = dict(params or {})
    while True:
        if cursor:
            p[cursor_key] = cursor
        data = api_get(path, p)
        items = data.get(list_key, []) or []
        for it in items:
            yield it
        cursor = data.get("cursor") or data.get("next_cursor")
        if not cursor:
            break

def shard_prefix(s: str) -> str:
    # shard by first two hex chars of sha1 for directory fanout
    return sha1_short(s)[:2]

def series_relpath(o: Dict[str, Any]) -> str:
    sid = str(o.get("ticker") or o.get("id") or sha1_short(json.dumps(o, sort_keys=True)))
    sp = shard_prefix(sid)
    return f"series/{sp}/{sid}.json"

def event_relpath(o: Dict[str, Any]) -> str:
    status = "closed" if (o.get("closed") or o.get("status") == "closed") else "open"
    y, m = yyyymm_from_obj(o)
    eid = str(o.get("ticker") or o.get("id") or sha1_short(json.dumps(o, sort_keys=True)))
    sp = shard_prefix(eid)
    return f"events/{status}/{y}/{m}/{sp}/{eid}.json"

def market_relpath(o: Dict[str, Any]) -> str:
    status = "closed" if (o.get("closed") or o.get("status") == "closed") else "open"
    y, m = yyyymm_from_obj(o)
    mid = str(o.get("ticker") or o.get("id") or sha1_short(json.dumps(o, sort_keys=True)))
    sp = shard_prefix(mid)
    return f"markets/{status}/{y}/{m}/{sp}/{mid}.json"

def year_from_relpath(rel: str) -> Optional[str]:
    parts = normalize_rel(rel).split("/")
    # events/<status>/<year>/...
    if len(parts) >= 3 and parts[0] in ("events", "markets"):
        y = parts[2]
        if y.isdigit() and len(y) == 4:
            return y
    return None

# -----------------------------
# State
# -----------------------------

def load_state() -> Dict[str, Any]:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_state(state: Dict[str, Any]) -> None:
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)

# -----------------------------
# Target repo selection + disk-based rollover
# -----------------------------

def should_roll_repo(local_repo_path: Path) -> bool:
    # roll if disk is low, or repo dir is too big
    free = disk_free_bytes(Path("."))
    if free < DISK_FREE_MIN_BYTES:
        return True
    # check repo size (best-effort with early stop)
    size = dir_size_bytes(local_repo_path, stop_at=REPO_MAX_BYTES + 1)
    return size > REPO_MAX_BYTES

def pick_repo_for(rel: str, kind: str, tracker: RepoRolloverTracker, wm: WriterManager) -> str:
    """
    Decide which GitHub repo to write to for a given relpath and kind.
    Applies year split + numbered suffix rollover.
    """
    rel = normalize_rel(rel)
    if kind == "series":
        return "Statground_Data_Kalshi_Series"

    y = year_from_relpath(rel)
    if kind == "event":
        base = "Statground_Data_Kalshi_Events"
    elif kind == "market":
        base = "Statground_Data_Kalshi_Markets"
    else:
        base = "Statground_Data_Kalshi_Unknown"

    prefix = f"{base}_{y}" if y else f"{base}_Current"
    repo = tracker.repo_name(prefix)

    # Ensure writer exists so we can check local path size if present
    w = wm.get(repo, f"Statground Kalshi {kind} data ({y or 'Current'})")
    # Open repo (clone) only when we actually write; but rollover wants local path
    if w.local_path.exists() and should_roll_repo(w.local_path):
        # close + delete to free disk, then bump index
        log.info("Rollover triggered for %s (disk low or repo too big).", repo)
        w.close_and_delete()
        tracker.bump(prefix)
        repo = tracker.repo_name(prefix)

    return repo

# -----------------------------
# Main crawl
# -----------------------------

def main() -> None:
    state = load_state()
    state.setdefault("cursors", {})
    tracker = RepoRolloverTracker(state)
    wm = WriterManager(OWNER, COMMIT_EVERY_FILES, MAX_OPEN_REPOS)
    repos_seen: set[str] = set()

    log.info("Kalshi fan-out crawler | owner=%s", OWNER)
    log.info("base_url=%s", BASE_URL)
    log.info("commit_every_files=%s max_open_repos=%s verbose_git=%s", COMMIT_EVERY_FILES, MAX_OPEN_REPOS, VERBOSE_GIT)
    log.info("logfile=%s", str(LOG_FILE))

    mode = "FULL(first run)" if not state.get("completed_full") else "INCR"
    log.info("mode=%s", mode)

    n_series = n_events = n_markets = 0

    try:
        # ---- SERIES ----
        for o in paginate("/series", "series"):
            rel = series_relpath(o)
            repo = pick_repo_for(rel, "series", tracker, wm)
            repos_seen.add(repo)
            w = wm.get(repo, "Statground Kalshi series data")
            w.write_json(rel, o, "series")
            w.maybe_flush(False)
            n_series += 1
            if n_series % 5000 == 0:
                log.info("series progress: %s", f"{n_series:,}")
                save_state(state)

        log.info("series done: %s", f"{n_series:,}")

        # ---- EVENTS ----
        for o in paginate("/events", "events"):
            rel = event_relpath(o)
            repo = pick_repo_for(rel, "event", tracker, wm)
            repos_seen.add(repo)
            w = wm.get(repo, "Statground Kalshi events data")
            w.write_json(rel, o, "event")
            w.maybe_flush(False)
            n_events += 1
            if n_events % 50000 == 0:
                log.info("events progress: %s", f"{n_events:,}")
                save_state(state)

        log.info("events done: %s", f"{n_events:,}")

        # ---- MARKETS ----
        for o in paginate("/markets", "markets"):
            rel = market_relpath(o)
            repo = pick_repo_for(rel, "market", tracker, wm)
            repos_seen.add(repo)
            w = wm.get(repo, "Statground Kalshi markets data")
            w.write_json(rel, o, "market")
            w.maybe_flush(False)
            n_markets += 1
            if n_markets % 50000 == 0:
                log.info("markets progress: %s", f"{n_markets:,}")
                save_state(state)

        log.info("markets done: %s", f"{n_markets:,}")

        # final flush
        wm.flush_all()

        # Write a manifest of repositories we touched this run.
        # This is used by the stats generator to avoid GitHub REST API listing.
        manifest = {
            "updated_utc": dt_utc_now_str(),
            "owner": OWNER,
            "repos": sorted(repos_seen),
        }
        Path("KALSHI_REPOS.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        state["completed_full"] = True
        save_state(state)

        # Commit/push manifest + state updates in this control repo (Statground_Data_Kalshi)
        git_commit_push_if_changed(Path("."), "kalshi: update manifest/state", CONTROL_OWNER, CONTROL_REPO)

    except Exception as e:
        log.error("FATAL: %s", e, exc_info=True)
        # attempt to flush what we can
        try:
            wm.flush_all()
        except Exception:
            pass
        save_state(state)
        raise
    finally:
        # Always close and delete local repos to reclaim disk (important for next steps).
        wm.close_all()

    log.info("DONE. series=%s events=%s markets=%s", f"{n_series:,}", f"{n_events:,}", f"{n_markets:,}")


if __name__ == "__main__":
    main()
