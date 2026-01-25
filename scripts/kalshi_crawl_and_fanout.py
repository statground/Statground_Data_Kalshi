#!/usr/bin/env python3
"""Kalshi crawler + fan-out publisher (AUTO-CREATE repos) — STREAMING PUSH (QUIET + LOGFILE)

This version reduces GitHub Actions console output to avoid log truncation.
- Console: periodic progress only
- File: detailed log at .work/logs/kalshi_run.log
- On error: prints last N lines of logfile to console

Env:
  GH_PAT (required)
  KALSHI_COMMIT_EVERY_FILES=5000
  KALSHI_MAX_OPEN_REPOS=3
  KALSHI_PRINT_EVERY_ITEMS=100000
  KALSHI_LOG_TAIL_ON_ERROR=200
  KALSHI_VERBOSE_GIT=0
"""

import os
import shutil
import hashlib, re, json, time, datetime, shutil, subprocess, logging
from pathlib import Path
from typing import Dict, Tuple, Optional
from collections import OrderedDict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
NOW_UTC = datetime.datetime.now(datetime.timezone.utc)

ORCH_ROOT = Path(".")
STATE_DIR = ORCH_ROOT / ".state"
STATE_FILE = STATE_DIR / "kalshi_state.json"
TARGETS_FILE = STATE_DIR / "kalshi_targets.json"

WORK_DIR = ORCH_ROOT / ".work"
REPOS_DIR = WORK_DIR / "repos"
LOG_DIR = WORK_DIR / "logs"
LOG_FILE = LOG_DIR / "kalshi_run.log"

LIMITS = {"events": 200, "markets": 1000}
EVENTS_BASE_PARAMS = {"with_nested_markets": "true"}

LOG_EVERY = int(os.getenv("KALSHI_LOG_EVERY", "50"))
SLEEP_SEC = float(os.getenv("KALSHI_SLEEP_SEC", "0.02"))

COMMIT_EVERY_FILES = int(os.getenv("KALSHI_COMMIT_EVERY_FILES", "5000"))
MAX_OPEN_REPOS = int(os.getenv("KALSHI_MAX_OPEN_REPOS", "3"))

# Rollover / disk safety
# - We cap per-repo growth (approx by bytes written and file count) to avoid exhausting the runner disk and git index limits.
# - "bytes written" is an approximation (JSON+meta payload sizes) but tracks disk growth closely enough for rollovers.
MAX_BYTES_PER_REPO = int(os.getenv("KALSHI_MAX_BYTES_PER_REPO", str(3_000_000_000)))  # ~3GB
MAX_FILES_PER_REPO = int(os.getenv("KALSHI_MAX_FILES_PER_REPO", str(250_000)))
MIN_FREE_GB = float(os.getenv("KALSHI_MIN_FREE_GB", "2.0"))  # global free space guardrail

GIT_PUSH_EVERY_SEC = int(os.getenv("KALSHI_GIT_PUSH_EVERY_SEC", "0"))

PRINT_EVERY_ITEMS = int(os.getenv("KALSHI_PRINT_EVERY_ITEMS", "100000"))
LOG_TAIL_ON_ERROR = int(os.getenv("KALSHI_LOG_TAIL_ON_ERROR", "200"))
VERBOSE_GIT = os.getenv("KALSHI_VERBOSE_GIT", "0").strip() == "1"

GH_PAT = os.getenv("GH_PAT", "").strip()
if not GH_PAT:
    raise RuntimeError("GH_PAT is required. Add it as Actions Secret 'GH_PAT'.")

GIT_AUTHOR_NAME = os.getenv("GIT_AUTHOR_NAME", "github-actions[bot]")
GIT_AUTHOR_EMAIL = os.getenv("GIT_AUTHOR_EMAIL", "github-actions[bot]@users.noreply.github.com")

GITHUB_API = "https://api.github.com"
DEFAULT_VISIBILITY_PRIVATE = False

def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("kalshi")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

log = setup_logging()

def tail_file(path: Path, n: int) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        return "".join(lines[-n:])
    except Exception as e:
        return f"(failed to read log tail: {e})"

def make_session():
    s = requests.Session()
    retry = Retry(
        total=10,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    return s

SESSION = make_session()

def kalshi_get_json(path: str, params: Optional[dict] = None) -> dict:
    r = SESSION.get(f"{BASE_URL}{path}", params=params or {}, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Kalshi HTTP {r.status_code}: {r.text[:800]}")
    return r.json()

def gh_headers():
    return {
        "Authorization": f"Bearer {GH_PAT}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "statground-kalshi-orchestrator",
    }

def gh_backoff_if_rate_limited(resp: requests.Response):
    if resp.status_code != 403:
        return
    if "rate limit exceeded" not in (resp.text or "").lower():
        return
    reset = resp.headers.get("x-ratelimit-reset")
    remaining = resp.headers.get("x-ratelimit-remaining")
    try:
        reset_ts = int(reset) if reset else None
    except Exception:
        reset_ts = None
    now = int(time.time())
    wait = min(60, max(5, (reset_ts - now + 2) if reset_ts and reset_ts > now else 30))
    log.warning("GitHub API rate limit hit (remaining=%s). Sleeping %ss and retrying...", remaining, wait)
    time.sleep(wait)

def gh_get(url: str) -> requests.Response:
    r = SESSION.get(url, headers=gh_headers(), timeout=60)
    if r.status_code == 403 and "rate limit exceeded" in (r.text or "").lower():
        gh_backoff_if_rate_limited(r)
        r = SESSION.get(url, headers=gh_headers(), timeout=60)
    return r

def gh_post(url: str, payload: dict) -> requests.Response:
    r = SESSION.post(url, headers=gh_headers(), json=payload, timeout=60)
    if r.status_code == 403 and "rate limit exceeded" in (r.text or "").lower():
        gh_backoff_if_rate_limited(r)
        r = SESSION.post(url, headers=gh_headers(), json=payload, timeout=60)
    return r

def sanitize(s):
    return re.sub(r"_+", "_", re.sub(r"[^\w\-.]+", "_", str(s)))[:180]

def parse_ym(v) -> Tuple[str, str]:
    try:
        if isinstance(v, (int, float)):
            if v > 1e12:
                v /= 1000
            dt = datetime.datetime.utcfromtimestamp(v)
        else:
            dt = datetime.datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return f"{dt.year:04d}", f"{dt.month:02d}"
    except Exception:
        return "unknown", "unknown"

def iso_to_unix_seconds(v) -> Optional[int]:
    if not v:
        return None
    try:
        dt = datetime.datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return None

def pick_ticker(o, keys):
    for k in keys:
        if o.get(k):
            return sanitize(o[k])
    return sanitize(abs(hash(json.dumps(o, sort_keys=True))))

def shard2(s: str) -> str:
    """Return a stable 2-char shard key to avoid huge directories on GitHub."""
    s = sanitize(s)
    if not s:
        return "00"
    # Use first 2 chars if alnum-heavy, else hash for better spread.
    head = s[:2].lower()
    if re.match(r"^[a-z0-9]{2}$", head):
        return head
    h = hashlib.sha1(s.encode("utf-8")).hexdigest()
    return h[:2]

def atomic_write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def rm_rf(p: Path):
    if p.exists():
        # Runner can keep temporary git files for a short time.
        shutil.rmtree(p, ignore_errors=True)

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.load(open(STATE_FILE, "r", encoding="utf-8"))
    return {
        "last_run_ts": None,
        "last_market_updated_ts": None,
        "first_full_done": False,
        "events_cursor": None,
        "events_page": 0,
        "events_total": 0,
        "markets_cursor": None,
        "markets_page": 0,
        "markets_total": 0,
    }

def save_state(state: dict):
    state = dict(state)
    state["last_success_utc"] = NOW_UTC.isoformat()
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    atomic_write_json(STATE_FILE, state)

def default_targets() -> dict:
    owner = os.getenv("GITHUB_REPOSITORY_OWNER", "statground")
    return {
        "owner": owner,
        "series_repo": "Statground_Data_Kalshi_Series",
        # Repositories are split by entity type (Events / Markets / Series) and by scope (Current / year),
        # and additionally use a rollover index suffix (_001, _002, ...) to stay disk-safe.
        "repo_prefix": "Statground_Data_Kalshi",
    }

def load_targets() -> dict:
    if TARGETS_FILE.exists():
        return json.load(open(TARGETS_FILE, "r", encoding="utf-8"))
    return default_targets()

def save_targets(targets: dict):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    atomic_write_json(TARGETS_FILE, targets)

_owner_type_cache: Dict[str, str] = {}
_repo_exists_cache: Dict[Tuple[str, str], bool] = {}
_repo_ensured: set = set()


class RepoRolloverTracker:
    """Tracks which {NNN} repo suffix is active per (kind, scope). Persisted in state.

    kind: 'Events' | 'Markets'
    scope: 'Current' | 'YYYY' | 'unknown'
    """
    def __init__(self, targets: dict, state: dict):
        self.targets = targets
        self.state = state
        self.prefix = targets.get("repo_prefix", "Statground_Data_Kalshi")
        # state["rollover"] stores current indices
        self.state.setdefault("rollover", {})  # e.g. {"Events:Current": 1, "Markets:2026": 2}

    def _key(self, kind: str, scope: str) -> str:
        return f"{kind}:{scope}"

    def index(self, kind: str, scope: str) -> int:
        return int(self.state["rollover"].get(self._key(kind, scope), 1))

    def bump(self, kind: str, scope: str) -> int:
        k = self._key(kind, scope)
        i = int(self.state["rollover"].get(k, 1)) + 1
        self.state["rollover"][k] = i
        return i

    def repo(self, kind: str, scope: str) -> str:
        # series is a singleton
        if kind == "Series" or scope == "Series":
            return self.targets.get("series_repo", f"{self.prefix}_Series")
        i = self.index(kind, scope)
        return f"{self.prefix}_{kind}_{scope}_{i:03d}"

def gh_owner_type(owner: str) -> str:
    if owner in _owner_type_cache:
        return _owner_type_cache[owner]
    r = gh_get(f"{GITHUB_API}/users/{owner}")
    if r.status_code >= 400:
        raise RuntimeError(f"GitHub: cannot read owner '{owner}': {r.status_code} {r.text[:300]}")
    t = r.json().get("type", "User")
    _owner_type_cache[owner] = t
    return t

def gh_repo_exists(owner: str, repo: str) -> bool:
    k = (owner, repo)
    if k in _repo_exists_cache:
        return _repo_exists_cache[k]
    r = gh_get(f"{GITHUB_API}/repos/{owner}/{repo}")
    if r.status_code == 200:
        _repo_exists_cache[k] = True
        return True
    if r.status_code == 404:
        _repo_exists_cache[k] = False
        return False
    raise RuntimeError(f"GitHub: repo check failed {owner}/{repo}: {r.status_code} {r.text[:300]}")
def gh_create_repo(owner: str, repo: str, description: str = ""):
    """Create a GitHub repo if needed.

    GitHub's create-repo endpoint can return 422 for several reasons, including
    'name already exists' or transient validation/race conditions. We treat any
    422 as 'maybe already exists' and re-check existence before failing.
    """
    url = "https://api.github.com/user/repos"
    payload = {"name": repo, "private": False, "description": description}

    def _post():
        return gh_post(url, payload)

    r = _post()
    if r.status_code in (201, 202):
        return

    if r.status_code == 422:
        # Race / eventual consistency / name already exists.
        for delay_s in (0, 1, 2, 4):
            if delay_s:
                time.sleep(delay_s)
            try:
                if gh_repo_exists(owner, repo):
                    log.info("Repo already exists (create returned 422): %s/%s", owner, repo)
                    _repo_exists_cache[(owner, repo)] = True
                    return
            except Exception:
                pass

        # One more retry of creation, then re-check.
        for delay_s in (2, 4):
            time.sleep(delay_s)
            r2 = _post()
            if r2.status_code in (201, 202):
                return
            try:
                if gh_repo_exists(owner, repo):
                    log.info("Repo exists after retry (create returned 422): %s/%s", owner, repo)
                    _repo_exists_cache[(owner, repo)] = True
                    return
            except Exception:
                pass

        # Fall through: genuine validation failure.
        raise RuntimeError(f"GitHub: repo create failed {owner}/{repo}: {r.status_code} {r.text[:500]}")

    raise RuntimeError(f"GitHub: repo create failed {owner}/{repo}: {r.status_code} {r.text[:500]}")
def ensure_repo(owner: str, repo: str, description: str = ""):
    k = (owner, repo)
    if k in _repo_ensured:
        return
    if gh_repo_exists(owner, repo):
        _repo_ensured.add(k)
        return
    log.info("Creating missing repo: %s/%s", owner, repo)
    # gh repo create requires --description, but we sometimes create repos
    # dynamically (e.g., sharded buckets) where a verbose description is not
    # necessary. Use an empty string in that case.
    gh_create_repo(owner, repo, description or "")
    _repo_ensured.add(k)

def series_relpath(o) -> Path:
    cat = sanitize((o.get("category") or "uncategorized").lower())
    sub = sanitize((o.get("subcategory") or "uncategorized").lower())
    t = pick_ticker(o, ["series_ticker", "ticker", "id"])
    return Path("series") / "by_category" / cat / sub / f"{t}.json"


OPEN_STATUSES = {"open", "active", "trading", "live"}


def _best_dt(o: dict, keys: list[str]) -> datetime.datetime | None:
    for k in keys:
        dt = _dt_from_any(o.get(k))
        if dt is not None:
            return dt
    return None


# Backward-compatible alias (older revisions used _pick_dt)
_pick_dt = _best_dt


def events_relpath(o) -> Path:
    """Shard events by status + YYYY/MM + 2-char prefix to avoid huge git trees."""
    status = (o.get("status") or "").lower()
    t = pick_ticker(o, ["ticker", "event_ticker", "id"])

    dt = _best_dt(
        o,
        [
            "strike_date",
            "close_time",
            "expiration_time",
            "settlement_time",
            "created_time",
            "created_at",
            "updated_time",
            "updated_at",
        ],
    )
    y, m = _ym_from_dt(dt)
    p2 = _shard2(t)

    if status in OPEN_STATUSES:
        return Path("events") / "open" / y / m / p2 / f"{t}.json"
    return Path("events") / "closed" / y / m / p2 / f"{t}.json"


def markets_relpath(o) -> Path:
    """Shard markets by status + YYYY/MM + 2-char prefix to avoid huge git trees."""
    status = (o.get("status") or "").lower()
    t = pick_ticker(o, ["ticker", "market_ticker", "id"])

    # Prefer a stable time for bucketing: close/expiration for closed,
    # updated/open time for open.
    dt = _best_dt(
        o,
        [
            "close_time",
            "expiration_time",
            "settlement_time",
            "created_time",
            "created_at",
            "updated_time",
            "updated_at",
            "strike_date",
        ],
    )
    y, m = _ym_from_dt(dt)
    p2 = _shard2(t)

    if status == "open":
        return Path("markets") / "open" / y / m / p2 / f"{t}.json"
    return Path("markets") / "closed" / y / m / p2 / f"{t}.json"


def _dt_from_any(v) -> datetime.datetime | None:
    """Best-effort parse of Kalshi-ish datetime values.

    Accepts:
      - ISO8601 strings (with or without Z, with or without timezone)
      - YYYY-MM-DD strings
      - unix epoch seconds/milliseconds
    """
    if v is None:
        return None
    try:
        # epoch seconds / ms
        if isinstance(v, (int, float)):
            ts = float(v)
            if ts > 10_000_000_000:  # ms
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        if not isinstance(v, str):
            return None

        s = v.strip()
        if not s:
            return None
        # date only
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        # normalize Z
        s2 = s.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s2)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None
    except Exception:
        return None


def _best_dt(o: dict, keys: list[str]) -> datetime.datetime | None:
    for k in keys:
        if k in o and o.get(k) not in (None, ""):
            dt = _dt_from_any(o.get(k))
            if dt is not None:
                return dt
    return None


# Backward-compatible alias (older revisions used _pick_dt)
_pick_dt = _best_dt


def _ym_from_dt(dt: datetime.datetime | None) -> tuple[str, str]:
    if dt is None:
        return ("unknown", "unknown")
    return (f"{dt.year:04d}", f"{dt.month:02d}")


def _shard2(s: str) -> str:
    """2-hex shard from SHA1(s). Stable and evenly distributed (avoids huge trees when tickers share prefixes like '2026...')."""
    s = sanitize(s)
    if not s:
        return "00"
    h = hashlib.sha1(s.encode("utf-8")).hexdigest()
    return h[:2]

def disk_free_gb(path: str) -> float:
    """Return free disk space in GB for the filesystem containing `path`."""
    try:
        du = shutil.disk_usage(path)
        return du.free / (1024**3)
    except Exception:
        return 0.0
def run(cmd, cwd=None, check=True, echo_to_console=False):
    """Run a command.
    - Always log command line to logfile
    - Capture stdout/stderr and append to logfile (DEBUG)
    - Optionally echo command line to console (INFO)
    """
    log.debug("$ %s", " ".join(cmd))
    if echo_to_console or VERBOSE_GIT:
        log.info("$ %s", " ".join(cmd))

    p = subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=True,
    )
    out = (p.stdout or "").strip()
    err = (p.stderr or "").strip()

    if out:
        log.debug("[stdout] %s", out if len(out) < 8000 else out[:8000] + "...(truncated)")
    if err:
        log.debug("[stderr] %s", err if len(err) < 8000 else err[:8000] + "...(truncated)")

    if check and p.returncode != 0:
        # log a concise error line too
        log.error("Command failed (%s): %s", p.returncode, " ".join(cmd))
        if err:
            log.error("stderr (tail): %s", err[-2000:])
        raise subprocess.CalledProcessError(p.returncode, cmd, output=p.stdout, stderr=p.stderr)

    return p

def repo_remote_url(owner: str, repo: str) -> str:
    return f"https://x-access-token:{GH_PAT}@github.com/{owner}/{repo}.git"

def git_config_identity(repo_path: Path):
    run(["git", "config", "user.name", GIT_AUTHOR_NAME], cwd=repo_path)
    run(["git", "config", "user.email", GIT_AUTHOR_EMAIL], cwd=repo_path)

def _chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def git_commit_push_if_changed(repo_path: Path, msg: str, paths: list[str] | None = None):
    # stage
    if paths:
        # Avoid `git add -A` on huge repos; only stage what we wrote.
        for chunk in _chunked(paths, 4000):
            run(["git", "add", "--"] + chunk, cwd=repo_path)
    else:
        run(["git", "add", "-A"], cwd=repo_path)

    # if nothing staged, stop
    r = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=repo_path)
    if r.returncode == 0:
        return False

    # commit
    run(["git", "commit", "--quiet", "-m", msg], cwd=repo_path)

    # IMPORTANT: actions/checkout may set http.https://github.com/.extraheader (GITHUB_TOKEN).
    # That can conflict with PAT-based push to other repos. Remove it locally before pushing.
    try:
        run(["git", "config", "--local", "--unset-all", "http.https://github.com/.extraheader"], cwd=repo_path, check=False)
    except Exception:
        pass

    # Also force-empty extra header for this push to prevent leakage.
    # (Git supports -c http.extraHeader= to override header for a single command.)
    # Push with retries (handles transient network errors and non-fast-forward when jobs overlap)
    last_err = None
    for attempt in range(1, 4):
        try:
            run(["git", "-c", "http.extraHeader=", "push"], cwd=repo_path)
            last_err = None
            break
        except subprocess.CalledProcessError as e:
            last_err = e
            err = (e.stderr or "").lower()
            # Common overlap case: remote has new commits (non-fast-forward)
            if "non-fast-forward" in err or "failed to push some refs" in err:
                log.warning("push rejected (non-fast-forward). Attempting pull --rebase then retry (%s/3)...", attempt)
                run(["git", "-c", "http.extraHeader=", "pull", "--rebase"], cwd=repo_path, check=False)
            # Transient network/HTTP
            elif "rpc failed" in err or "connection" in err or "timeout" in err or "http 5" in err:
                log.warning("push transient error. Sleeping and retrying (%s/3)...", attempt)
                time.sleep(5 * attempt)
            else:
                log.warning("push failed (attempt %s/3). stderr tail: %s", attempt, (e.stderr or "")[-800:])
                time.sleep(2)
    if last_err is not None:
        raise last_err

    return True


class RolloverRequested(Exception):
    """Internal signal: current writer hit rollover threshold."""
    pass


class RepoWriter:
    def __init__(self, owner: str, repo: str, local_path: str, tracker: RepoRolloverTracker):
        self.owner = owner
        self.repo = repo
        self.local_path = local_path
        self.tracker = tracker
        self.files_written = 0
        self.last_flush_files = 0
        self._opened = False
        # Only stage/commit files we write in this process. This avoids staging deletions
        # when we clean up local files to keep runner disk usage low.
        self._pending_paths: list[str] = []

    def open(self):
        if self._opened:
            return
        ensure_local_repo(self.owner, self.repo, self.local_path)
        self._opened = True

    def write_json(self, relpath: str, obj: dict, pretty: bool = False):
        self.open()
        p = Path(self.local_path) / relpath
        p.parent.mkdir(parents=True, exist_ok=True)
        data = json.dumps(obj, ensure_ascii=False, indent=(2 if pretty else None), sort_keys=False)
        p.write_text(data + "\n", encoding="utf-8")
        self.files_written += 1
        self._pending_paths.append(relpath)

    def maybe_flush(self, force: bool):
        # Flush when we have enough pending files or on force.
        if not self._opened:
            return
        pending = len(self._pending_paths)
        if not force and pending < COMMIT_EVERY_FILES:
            return

        # Always include counts file if present
        counts_path = "KALSHI_COUNTS.json"
        if (Path(self.local_path) / counts_path).exists():
            self._pending_paths.append(counts_path)

        # De-duplicate and chunk git add to avoid command length limits
        unique_paths = []
        seen = set()
        for rp in self._pending_paths:
            if rp not in seen:
                seen.add(rp)
                unique_paths.append(rp)

        changed = git_commit_push_if_changed(
            self.local_path,
            f"kalshi: update data ({NOW_UTC.strftime('%Y-%m-%d %H:%M UTC')})",
            paths=unique_paths,
        )

        # After push, aggressively delete data files from local worktree to keep runner disk usage low.
        # We DO NOT stage these deletions (since we only 'git add' the paths above), so remote history is preserved.
        if changed:
            for rp in unique_paths:
                if rp == counts_path:
                    continue
                fp = Path(self.local_path) / rp
                try:
                    if fp.exists() and fp.is_file():
                        fp.unlink()
                except Exception:
                    pass
            # Best-effort: remove empty directories
            try:
                for root, dirs, files in os.walk(self.local_path, topdown=False):
                    if root.endswith(os.sep + ".git") or f"{os.sep}.git{os.sep}" in root:
                        continue
                    if not dirs and not files:
                        try:
                            os.rmdir(root)
                        except Exception:
                            pass
            except Exception:
                pass

        self.last_flush_files = self.files_written
        self._pending_paths = []

    def close(self):
        try:
            self.maybe_flush(True)
        finally:
            # Always remove local worktree to free runner disk
            try:
                shutil.rmtree(self.local_path, ignore_errors=True)
            except Exception:
                pass
class WriterManager:
    """Manages a small number of open local git worktrees to stay within runner disk limits."""

    def __init__(self, owner: str, tracker: RepoRolloverTracker, max_open_repos: int = MAX_OPEN_REPOS):
        self.owner = owner
        self.tracker = tracker
        self.max_open_repos = max_open_repos
        self.writers: dict[str, RepoWriter] = {}
        self.open_order: list[str] = []  # LRU-ish

    def get(self, repo: str) -> RepoWriter:
        if repo in self.writers:
            # touch for LRU
            if repo in self.open_order:
                self.open_order.remove(repo)
            self.open_order.append(repo)
            return self.writers[repo]

        # Evict if needed
        while len(self.open_order) >= self.max_open_repos:
            evict_repo = self.open_order.pop(0)
            w = self.writers.pop(evict_repo, None)
            if w:
                log.info("Closing repo: %s", evict_repo)
                w.close()

        local_path = os.path.join(WORK_REPOS_DIR, repo)
        w = RepoWriter(self.owner, repo, local_path, self.tracker)
        w.open()
        self.writers[repo] = w
        self.open_order.append(repo)
        return w

    
    def flush_all(self):
        for repo in list(self.open_order):
            w = self.writers.get(repo)
            if w:
                try:
                    w.maybe_flush(force=True)
                except RolloverRequested:
                    # If flush triggers rollover, just close; next write will open new repo.
                    w.close()
                    self.writers.pop(repo, None)
                    if repo in self.open_order:
                        self.open_order.remove(repo)

def close_all(self):
        for repo in list(self.open_order):
            w = self.writers.get(repo)
            if w:
                log.info("Closing repo: %s", repo)
                w.close()
        self.writers.clear()
        self.open_order.clear()



def crawl_series_all(yield_item) -> int:
    data = kalshi_get_json("/series", params={})
    items = data.get("series", [])
    for o in items:
        yield_item(series_relpath(o), o)
    return len(items)

def crawl_events_full(yield_item, state: dict) -> int:
    cursor = state.get("events_cursor")
    page = int(state.get("events_page") or 0)
    total = int(state.get("events_total") or 0)
    log.info("[events:full] resume page=%s cursor=%s total=%s", f"{page:,}", "YES" if cursor else "NO", f"{total:,}")
    while True:
        params = dict(EVENTS_BASE_PARAMS); params["limit"] = LIMITS["events"]
        if cursor: params["cursor"] = cursor
        data = kalshi_get_json("/events", params=params)
        items = data.get("events", []); next_cursor = data.get("cursor")
        if not items and not next_cursor: break
        for o in items: yield_item(events_relpath(o), o)
        total += len(items); page += 1; cursor = next_cursor
        state.update({"events_cursor": cursor, "events_page": page, "events_total": total}); save_state(state)
        if page % LOG_EVERY == 0: log.info("[events:full] page=%s total=%s", f"{page:,}", f"{total:,}")
        if not cursor: break
        time.sleep(SLEEP_SEC)
    state["events_cursor"] = None; save_state(state)
    return total

def crawl_markets_full(yield_item, state: dict) -> int:
    cursor = state.get("markets_cursor")
    page = int(state.get("markets_page") or 0)
    total = int(state.get("markets_total") or 0)
    max_seen_updated = int(state.get("last_market_updated_ts") or 0)
    log.info("[markets:full] resume page=%s cursor=%s total=%s", f"{page:,}", "YES" if cursor else "NO", f"{total:,}")
    while True:
        params = {"limit": LIMITS["markets"]}
        if cursor: params["cursor"] = cursor
        data = kalshi_get_json("/markets", params=params)
        items = data.get("markets", []); next_cursor = data.get("cursor")
        if not items and not next_cursor: break
        for o in items:
            yield_item(markets_relpath(o), o)
            u = iso_to_unix_seconds(o.get("updated_time"))
            if u and u > max_seen_updated: max_seen_updated = u
        total += len(items); page += 1; cursor = next_cursor
        state.update({"markets_cursor": cursor, "markets_page": page, "markets_total": total, "last_market_updated_ts": max_seen_updated}); save_state(state)
        if page % LOG_EVERY == 0: log.info("[markets:full] page=%s total=%s", f"{page:,}", f"{total:,}")
        if not cursor: break
        time.sleep(SLEEP_SEC)
    state["markets_cursor"] = None
    state["last_market_updated_ts"] = max_seen_updated or int(NOW_UTC.timestamp())
    save_state(state)
    return total

def main():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    REPOS_DIR.mkdir(parents=True, exist_ok=True)

    state = load_state()
    targets = load_targets()

    owner = targets.get("owner") or os.getenv("GITHUB_REPOSITORY_OWNER", "statground")
    targets["owner"] = owner
    targets.setdefault("repo_prefix", "Statground_Data_Kalshi")
    targets.setdefault("series_repo", f"{targets['repo_prefix']}_Series")

    log.info("Kalshi fan-out crawler | owner=%s", owner)
    log.info("base_url=%s", BASE_URL)
    log.info("commit_every_files=%s max_open_repos=%s verbose_git=%s", COMMIT_EVERY_FILES, MAX_OPEN_REPOS, VERBOSE_GIT)
    log.info("logfile=%s", str(LOG_FILE))

    ensure_repo(owner, targets["series_repo"], "Kalshi series snapshot (auto-created)")

    tracker = RepoRolloverTracker(targets, state)
    wm = WriterManager(owner, tracker)
    progress = {"series": 0, "events": 0, "markets": 0, "total": 0, "last_print": 0}

    def maybe_print_progress(force=False):
        if force or (progress["total"] - progress["last_print"] >= PRINT_EVERY_ITEMS):
            log.info("progress total=%s (series=%s, events=%s, markets=%s)",
                     f"{progress['total']:,}", f"{progress['series']:,}", f"{progress['events']:,}", f"{progress['markets']:,}")
            progress["last_print"] = progress["total"]

    def yield_item(rel: Path, obj: dict):
        nonlocal tracker
        repo = target_repo_for_relpath(rel, tracker)

        while True:
            w = wm.get(repo)
            try:
                w.write_json(str(rel), obj)
                w.maybe_flush(False)
                break
            except RolloverRequested:
                # Close and advance to next repo index for this (kind, scope)
                kind, scope = w.kind, w.scope
                log.warning("[rollover] triggered for %s (kind=%s scope=%s). Switching repo index.", repo, kind, scope)
                w.close()
                # remove writer from manager
                wm.writers.pop(repo, None)
                if repo in wm.open_order:
                    wm.open_order.remove(repo)
                if kind != "Series":
                    tracker.bump(kind, scope)
                    repo = tracker.repo(kind, scope)
                else:
                    # should never happen, but avoid infinite loops
                    raise

        kind0 = rel.parts[0]
        if kind0 in progress:
            progress[kind0] += 1
        progress["total"] += 1
        maybe_print_progress(False)

    first_run = not state.get("first_full_done")
    log.info("mode=%s", "FULL(first run)" if first_run else "FULL(resume/refresh)")

    try:
        n_series = crawl_series_all(yield_item); log.info("series done: %s", f"{n_series:,}"); maybe_print_progress(True)
        n_events = crawl_events_full(yield_item, state); log.info("events(full) done: %s", f"{n_events:,}"); maybe_print_progress(True)
        n_markets = crawl_markets_full(yield_item, state); log.info("markets(full) done: %s", f"{n_markets:,}"); maybe_print_progress(True)
        wm.flush_all()
        state["first_full_done"] = True; state["last_run_ts"] = int(NOW_UTC.timestamp()); save_state(state)
        save_targets(targets)
        atomic_write_json(ORCH_ROOT / "manifest.json", {
            "base_url": BASE_URL,
            "mode": "fanout_streaming_push_quiet",
            "last_run_utc": NOW_UTC.isoformat(),
            "commit_every_files": COMMIT_EVERY_FILES,
            "max_open_repos": MAX_OPEN_REPOS,
        })
        log.info("DONE – fan-out crawl complete (streaming push, quiet)")
    finally:
        wm.close_all()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        if isinstance(e, subprocess.CalledProcessError):
            print("\n========== FAILED COMMAND ==========")
            try:
                print("CMD:", " ".join(e.cmd) if isinstance(e.cmd, (list, tuple)) else str(e.cmd))
            except Exception:
                pass
            if e.stderr:
                print("\n--- stderr (tail) ---")
                print((e.stderr or "")[-4000:])
            if e.output:
                print("\n--- stdout (tail) ---")
                out = e.output if isinstance(e.output, str) else str(e.output)
                print(out[-2000:])
            print("===================================\n")
        log.exception("FATAL: %s", e)
        print("\n========== LOG TAIL (last lines) ==========")
        print(tail_file(LOG_FILE, LOG_TAIL_ON_ERROR))
        print("==========================================\n")
        raise