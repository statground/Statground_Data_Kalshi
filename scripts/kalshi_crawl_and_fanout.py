#!/usr/bin/env python3
"""
Kalshi crawler + fan-out publisher (AUTO-CREATE repos)

What changed vs previous version
- If target repos (Current / Year) do NOT exist, this script creates them via GitHub REST API.
- Uses GH_PAT (fine-grained, All repositories) to:
  - check repo exists
  - create repo under org/user
  - clone/push data

Fan-out rules
- series + open events/markets -> Current repo
- closed events/markets -> Year repo by year extracted from path

State files (committed in orchestrator)
- .state/kalshi_state.json
- .state/kalshi_targets.json  (owner/current_repo/year_repos mapping, auto-updated)

IMPORTANT
- For org owners, your org settings must allow you (token owner) to create repos.
"""

import os, re, json, time, datetime, shutil, subprocess
from pathlib import Path
from typing import Dict, Tuple, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -------------------------
# Config
# -------------------------
BASE_URL = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
NOW_UTC = datetime.datetime.now(datetime.timezone.utc)

ORCH_ROOT = Path(".")
STATE_DIR = ORCH_ROOT / ".state"
STATE_FILE = STATE_DIR / "kalshi_state.json"
TARGETS_FILE = STATE_DIR / "kalshi_targets.json"

WORK_DIR = ORCH_ROOT / ".work"
STAGE_DIR = WORK_DIR / "stage"

LIMITS = {"events": 200, "markets": 1000}
EVENTS_BASE_PARAMS = {"with_nested_markets": "true"}

LOG_EVERY = int(os.getenv("KALSHI_LOG_EVERY", "50"))
SLEEP_SEC = float(os.getenv("KALSHI_SLEEP_SEC", "0.05"))

EVENT_BACKFILL_SEC = int(os.getenv("KALSHI_EVENT_BACKFILL_SEC", "172800"))
MARKET_BACKFILL_SEC = int(os.getenv("KALSHI_MARKET_BACKFILL_SEC", "120"))

GH_PAT = os.getenv("GH_PAT", "").strip()
if not GH_PAT:
    raise RuntimeError("GH_PAT is required. Add it as Actions Secret 'GH_PAT'.")

GIT_AUTHOR_NAME = os.getenv("GIT_AUTHOR_NAME", "github-actions[bot]")
GIT_AUTHOR_EMAIL = os.getenv("GIT_AUTHOR_EMAIL", "github-actions[bot]@users.noreply.github.com")

GITHUB_API = "https://api.github.com"
DEFAULT_VISIBILITY_PRIVATE = False  # set True if you prefer private repos

# -------------------------
# HTTP sessions
# -------------------------
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

def gh_get(url: str):
    r = SESSION.get(url, headers=gh_headers(), timeout=60)
    return r

def gh_post(url: str, payload: dict):
    r = SESSION.post(url, headers=gh_headers(), json=payload, timeout=60)
    return r

# -------------------------
# Utils
# -------------------------
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

def write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def rm_rf(p: Path):
    if p.exists():
        shutil.rmtree(p)

# -------------------------
# State / Targets
# -------------------------
def load_state() -> dict:
    if STATE_FILE.exists():
        return json.load(open(STATE_FILE, "r", encoding="utf-8"))
    return {
        "last_run_ts": None,
        "last_market_updated_ts": None,
        "last_success_utc": None,
        "first_full_done": False,
    }

def save_state(state: dict):
    state = dict(state)
    state["last_success_utc"] = NOW_UTC.isoformat()
    write_json(STATE_FILE, state)

def default_targets() -> dict:
    owner = os.getenv("GITHUB_REPOSITORY_OWNER", "statground")
    y = NOW_UTC.year
    return {
        "owner": owner,
        "current_repo": "Statground_Data_Kalshi_Current",
        "year_repos": {
            str(y): f"Statground_Data_Kalshi_{y}",
            str(y + 1): f"Statground_Data_Kalshi_{y + 1}",
        },
    }

def load_targets() -> dict:
    if TARGETS_FILE.exists():
        return json.load(open(TARGETS_FILE, "r", encoding="utf-8"))
    return default_targets()

def save_targets(targets: dict):
    write_json(TARGETS_FILE, targets)

# -------------------------
# GitHub repo auto-create
# -------------------------
def gh_owner_type(owner: str) -> str:
    # returns "Organization" or "User"
    r = gh_get(f"{GITHUB_API}/users/{owner}")
    if r.status_code >= 400:
        raise RuntimeError(f"GitHub: cannot read owner '{owner}': {r.status_code} {r.text[:300]}")
    return r.json().get("type", "User")

def gh_repo_exists(owner: str, repo: str) -> bool:
    r = gh_get(f"{GITHUB_API}/repos/{owner}/{repo}")
    if r.status_code == 200:
        return True
    if r.status_code == 404:
        return False
    raise RuntimeError(f"GitHub: repo check failed {owner}/{repo}: {r.status_code} {r.text[:300]}")

def gh_create_repo(owner: str, repo: str, description: str):
    otype = gh_owner_type(owner)
    payload = {
        "name": repo,
        "description": description,
        "private": bool(DEFAULT_VISIBILITY_PRIVATE),
        "auto_init": True,
        "has_issues": False,
        "has_projects": False,
        "has_wiki": False,
    }
    if otype == "Organization":
        url = f"{GITHUB_API}/orgs/{owner}/repos"
    else:
        # for user-owned repos, token owner must be same user
        url = f"{GITHUB_API}/user/repos"
    r = gh_post(url, payload)
    if r.status_code in (201,):
        print(f"✅ Created repo: {owner}/{repo}")
        return
    # If already exists (race), treat as ok
    if r.status_code == 422 and "already exists" in (r.text or "").lower():
        print(f"ℹ️ Repo already exists (race): {owner}/{repo}")
        return
    raise RuntimeError(f"GitHub: repo create failed {owner}/{repo}: {r.status_code} {r.text[:500]}")

def ensure_repo(owner: str, repo: str, description: str):
    if gh_repo_exists(owner, repo):
        return
    print(f"▶ Creating missing repo: {owner}/{repo}")
    gh_create_repo(owner, repo, description)

# -------------------------
# Path mapping (Kalshi style)
# -------------------------
def series_relpath(o) -> Path:
    cat = sanitize((o.get("category") or "uncategorized").lower())
    sub = sanitize((o.get("subcategory") or "uncategorized").lower())
    t = pick_ticker(o, ["series_ticker", "ticker", "id"])
    return Path("series") / "by_category" / cat / sub / f"{t}.json"

def events_relpath(o) -> Path:
    t = pick_ticker(o, ["event_ticker", "ticker", "id"])
    if (o.get("status") or "").lower() == "open":
        return Path("events") / "open" / f"{t}.json"
    y, m = parse_ym(o.get("strike_date"))
    return Path("events") / "closed" / y / m / f"{t}.json"

def markets_relpath(o) -> Path:
    t = pick_ticker(o, ["ticker", "market_ticker", "id"])
    status = (o.get("status") or "").lower()
    if status in ("open", "unopened", "paused") or not o.get("close_time"):
        return Path("markets") / "open" / f"{t}.json"
    y, m = parse_ym(o.get("close_time"))
    return Path("markets") / "closed" / y / m / f"{t}.json"

def infer_year_repo(targets: dict, year: str) -> str:
    # If mapping missing, create default repo name and store mapping.
    yr = str(year)
    if "year_repos" not in targets:
        targets["year_repos"] = {}
    if yr not in targets["year_repos"]:
        targets["year_repos"][yr] = f"Statground_Data_Kalshi_{yr}"
    return targets["year_repos"][yr]

def target_repo_for_relpath(rel: Path, targets: dict) -> str:
    if rel.parts[0] == "series":
        return targets["current_repo"]
    if len(rel.parts) >= 2 and rel.parts[1] == "open":
        return targets["current_repo"]
    if len(rel.parts) >= 4 and rel.parts[1] == "closed":
        year = rel.parts[2]
        return infer_year_repo(targets, year)
    return targets["current_repo"]

# -------------------------
# Crawl functions
# -------------------------
def crawl_series_all(yield_item) -> int:
    data = kalshi_get_json("/series", params={})
    items = data.get("series", [])
    for o in items:
        yield_item(series_relpath(o), o)
    return len(items)

def crawl_events_full(yield_item) -> int:
    cursor = None
    page = 0
    total = 0
    while True:
        params = dict(EVENTS_BASE_PARAMS)
        params["limit"] = LIMITS["events"]
        if cursor:
            params["cursor"] = cursor
        data = kalshi_get_json("/events", params=params)
        items = data.get("events", [])
        next_cursor = data.get("cursor")
        if not items and not next_cursor:
            break
        for o in items:
            yield_item(events_relpath(o), o)
        total += len(items)
        page += 1
        if page % LOG_EVERY == 0:
            print(f"[events:full] page={page:,} total={total:,}")
        cursor = next_cursor
        if not cursor:
            break
        time.sleep(SLEEP_SEC)
    return total

def crawl_markets_full(yield_item, state: dict) -> int:
    cursor = None
    page = 0
    total = 0
    max_seen_updated = state.get("last_market_updated_ts") or 0

    while True:
        params = {"limit": LIMITS["markets"]}
        if cursor:
            params["cursor"] = cursor
        data = kalshi_get_json("/markets", params=params)
        items = data.get("markets", [])
        next_cursor = data.get("cursor")
        if not items and not next_cursor:
            break
        for o in items:
            yield_item(markets_relpath(o), o)
            u = iso_to_unix_seconds(o.get("updated_time"))
            if u and u > max_seen_updated:
                max_seen_updated = u
        total += len(items)
        page += 1
        if page % LOG_EVERY == 0:
            print(f"[markets:full] page={page:,} total={total:,}")
        cursor = next_cursor
        if not cursor:
            break
        time.sleep(SLEEP_SEC)

    state["last_market_updated_ts"] = int(max_seen_updated) if max_seen_updated else int(NOW_UTC.timestamp())
    return total

def crawl_markets_incremental(yield_item, state: dict) -> int:
    last_u = state.get("last_market_updated_ts")
    if not last_u:
        print("[markets:inc] baseline missing -> full")
        return crawl_markets_full(yield_item, state)

    min_updated_ts = max(0, int(last_u) - MARKET_BACKFILL_SEC)
    cursor = None
    page = 0
    total = 0
    max_seen_updated = int(last_u)

    while True:
        params = {"limit": LIMITS["markets"], "min_updated_ts": min_updated_ts}
        if cursor:
            params["cursor"] = cursor
        data = kalshi_get_json("/markets", params=params)
        items = data.get("markets", [])
        next_cursor = data.get("cursor")
        if not items and not next_cursor:
            break

        for o in items:
            yield_item(markets_relpath(o), o)
            u = iso_to_unix_seconds(o.get("updated_time"))
            if u and u > max_seen_updated:
                max_seen_updated = u

        total += len(items)
        page += 1
        if page % LOG_EVERY == 0:
            print(f"[markets:inc] page={page:,} total={total:,}")
        cursor = next_cursor
        if not cursor:
            break
        time.sleep(SLEEP_SEC)

    state["last_market_updated_ts"] = max_seen_updated
    return total

def crawl_events_incremental(yield_item, state: dict) -> int:
    last_run = state.get("last_run_ts")
    if not last_run:
        print("[events:inc] baseline missing -> full")
        return crawl_events_full(yield_item)

    min_close_ts = max(0, int(last_run) - EVENT_BACKFILL_SEC)

    def crawl_status(status: str, extra_params: dict) -> int:
        cursor = None
        page = 0
        total = 0
        while True:
            params = dict(EVENTS_BASE_PARAMS)
            params["limit"] = LIMITS["events"]
            params["status"] = status
            params.update(extra_params)
            if cursor:
                params["cursor"] = cursor
            data = kalshi_get_json("/events", params=params)
            items = data.get("events", [])
            next_cursor = data.get("cursor")
            if not items and not next_cursor:
                break
            for o in items:
                yield_item(events_relpath(o), o)
            total += len(items)
            page += 1
            if page % LOG_EVERY == 0:
                print(f"[events:{status}] page={page:,} total={total:,}")
            cursor = next_cursor
            if not cursor:
                break
            time.sleep(SLEEP_SEC)
        return total

    total_open = crawl_status("open", {})
    total_closed = crawl_status("closed", {"min_close_ts": min_close_ts})
    total_settled = crawl_status("settled", {"min_close_ts": min_close_ts})
    return total_open + total_closed + total_settled

# -------------------------
# Staging / counts
# -------------------------
def write_counts(repo_dir: Path) -> dict:
    counts = {"series": 0, "events_open": 0, "events_closed": 0, "markets_open": 0, "markets_closed": 0, "files_total": 0}
    for p in repo_dir.rglob("*.json"):
        rp = p.relative_to(repo_dir).as_posix()
        counts["files_total"] += 1
        if rp.startswith("series/"):
            counts["series"] += 1
        elif rp.startswith("events/open/"):
            counts["events_open"] += 1
        elif rp.startswith("events/closed/"):
            counts["events_closed"] += 1
        elif rp.startswith("markets/open/"):
            counts["markets_open"] += 1
        elif rp.startswith("markets/closed/"):
            counts["markets_closed"] += 1
    counts["generated_utc"] = NOW_UTC.isoformat()
    return counts

# -------------------------
# Git helpers
# -------------------------
def run(cmd, cwd=None):
    print(f"$ {' '.join(cmd)}")
    subprocess.run(cmd, cwd=cwd, check=True)

def clone_repo(owner: str, repo: str, dest: Path):
    rm_rf(dest)
    url = f"https://x-access-token:{GH_PAT}@github.com/{owner}/{repo}.git"
    run(["git", "clone", "--depth", "1", url, str(dest)])

def sync_tree(src: Path, dst: Path):
    for p in src.rglob("*"):
        if p.is_dir():
            continue
        rel = p.relative_to(src)
        out = dst / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(p, out)

def commit_push_if_changed(repo_path: Path, msg: str):
    run(["git", "config", "user.name", GIT_AUTHOR_NAME], cwd=repo_path)
    run(["git", "config", "user.email", GIT_AUTHOR_EMAIL], cwd=repo_path)
    run(["git", "add", "-A"], cwd=repo_path)

    r = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=repo_path)
    if r.returncode == 0:
        print("No changes to commit.")
        return

    run(["git", "commit", "-m", msg], cwd=repo_path)
    run(["git", "push"], cwd=repo_path)

# -------------------------
# Main
# -------------------------
def main():
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    # workspace reset
    rm_rf(WORK_DIR)
    STAGE_DIR.mkdir(parents=True, exist_ok=True)

    state = load_state()
    targets = load_targets()

    owner = targets.get("owner") or os.getenv("GITHUB_REPOSITORY_OWNER", "statground")
    targets["owner"] = owner
    if "current_repo" not in targets or not targets["current_repo"]:
        targets["current_repo"] = "Statground_Data_Kalshi_Current"
    if "year_repos" not in targets:
        targets["year_repos"] = {}

    # ensure repos exist (current + known year repos)
    ensure_repo(owner, targets["current_repo"], "Kalshi current snapshot (series + open events/markets)")
    for y, rname in list(targets["year_repos"].items()):
        ensure_repo(owner, rname, f"Kalshi closed archive for year {y}")

    # yield_item closure uses updated targets
    def yield_item(rel: Path, obj: dict):
        repo = target_repo_for_relpath(rel, targets)
        # if new year inferred, ensure repo exists now
        if repo not in [targets["current_repo"]] and repo not in targets["year_repos"].values():
            # should not happen; defensive
            pass
        # if year mapping newly added, ensure repo exists
        if repo.startswith("Statground_Data_Kalshi_") and repo != targets["current_repo"]:
            ensure_repo(owner, repo, f"Kalshi closed archive (auto-created)")
        out = STAGE_DIR / repo / rel
        write_json(out, obj)

    first_run = not state.get("first_full_done")
    print(f"▶ Kalshi fan-out crawler | owner={owner}")
    print(f"▶ mode={'FULL(first run)' if first_run else 'INCREMENTAL'}")
    print(f"▶ base_url={BASE_URL}")

    n_series = crawl_series_all(yield_item)
    print(f"✅ series: {n_series:,}")

    if first_run:
        n_events = crawl_events_full(yield_item)
        print(f"✅ events(full): {n_events:,}")
        n_markets = crawl_markets_full(yield_item, state)
        print(f"✅ markets(full): {n_markets:,}")
        state["first_full_done"] = True
    else:
        n_events = crawl_events_incremental(yield_item, state)
        print(f"✅ events(inc): {n_events:,}")
        n_markets = crawl_markets_incremental(yield_item, state)
        print(f"✅ markets(inc): {n_markets:,}")

    state["last_run_ts"] = int(NOW_UTC.timestamp())
    save_state(state)

    # After crawl, persist targets (may include newly inferred year repos)
    # Ensure all newly added year repos exist
    for y, rname in list(targets.get("year_repos", {}).items()):
        ensure_repo(owner, rname, f"Kalshi closed archive for year {y}")
    save_targets(targets)

    # Push staged changes
    staged_repos = [p.name for p in STAGE_DIR.iterdir() if p.is_dir()]
    print(f"▶ staged repos: {staged_repos}")

    for repo in staged_repos:
        src = STAGE_DIR / repo
        local = WORK_DIR / "repos" / repo
        ensure_repo(owner, repo, "Kalshi data repo (auto)")
        clone_repo(owner, repo, local)
        sync_tree(src, local)

        counts = write_counts(local)
        write_json(local / "KALSHI_COUNTS.json", counts)

        commit_push_if_changed(local, f"kalshi: update data ({NOW_UTC.strftime('%Y-%m-%d %H:%M UTC')})")

    # Orchestrator stats
    stats_md = ORCH_ROOT / "KALSHI_REPO_STATS.md"
    lines = []
    lines.append("# Kalshi Data Repos\n\n")
    lines.append(f"- Updated (UTC): `{NOW_UTC.isoformat()}`\n")
    lines.append(f"- Base URL: `{BASE_URL}`\n")
    lines.append(f"- Current repo: `{targets['current_repo']}`\n")
    lines.append("- Year repos:\n")
    for y, rname in sorted(targets.get("year_repos", {}).items()):
        lines.append(f"  - {y}: `{rname}`\n")
    stats_md.write_text("".join(lines), encoding="utf-8")

    # Orchestrator manifest
    write_json(ORCH_ROOT / "manifest.json", {
        "base_url": BASE_URL,
        "mode": "fanout_autocreate_repos",
        "last_run_utc": NOW_UTC.isoformat(),
        "state_file": str(STATE_FILE),
        "targets_file": str(TARGETS_FILE),
        "notes": {
            "markets_incremental": "min_updated_ts",
            "events_incremental": "open all + closed/settled filtered by min_close_ts backfill",
            "series": "full refresh",
            "repo_autocreate": True
        }
    })

    print("🎉 DONE – fan-out crawl complete (auto-create enabled)")

if __name__ == "__main__":
    main()
