#!/usr/bin/env python3
import base64
import datetime as dt
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import requests

# Output file written into the CURRENT repo (this workflow's checkout)
STATS_MD_FILENAME = "KALSHI_REPO_STATS.md"


def utc_now_str() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def gh_headers() -> Dict[str, str]:
    token = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN") or ""
    h = {"Accept": "application/vnd.github+json"}
    if token:
        h["Authorization"] = f"token {token}"
    return h



def gh_list_repos_by_prefix(owner: str, prefix: str) -> List[str]:
    """List repos for `owner` whose names start with `prefix`.

    NOTE: The /users/{owner}/repos endpoint only returns PUBLIC repos.
    Since these data repos are typically PRIVATE, we prefer /user/repos when a token is present.
    """
    token = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")
    out: List[str] = []

    if token:
        # Authenticated: include private repos owned by the authenticated user
        url = "https://api.github.com/user/repos?per_page=100&affiliation=owner"
    else:
        url = f"https://api.github.com/users/{owner}/repos?per_page=100&type=owner"

    while url:
        r = requests.get(url, headers=gh_headers(), timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"GitHub list repos failed {r.status_code}: {r.text[:200]}")
        for repo in r.json():
            name = repo.get("name", "")
            if name.startswith(prefix):
                out.append(name)
        # pagination
        link = r.headers.get("Link", "")
        next_url = None
        if link:
            for part in link.split(","):
                part = part.strip()
                if part.endswith('rel="next"'):
                    next_url = part.split(";")[0].strip()[1:-1]
                    break
        url = next_url

    return sorted(set(out))

def run(cmd: List[str], cwd: str | None = None) -> str:
    p = subprocess.run(cmd, cwd=cwd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.stdout


def count_repo(repo_url: str, tmp_dir: Path) -> Dict[str, int]:
    # Clone depth-1; do NOT checkout files (we only need the tree listing).
    run(["git", "clone", "--depth", "1", "--filter=blob:none", repo_url, str(tmp_dir)])
    # List all paths in HEAD
    names = run(["git", "ls-tree", "-r", "--name-only", "HEAD"], cwd=str(tmp_dir)).splitlines()
    total_files = len(names)
    json_files = 0
    events_json = 0
    markets_json = 0
    series_json = 0
    unknown_json = 0

    for n in names:
        if not n.endswith(".json"):
            continue
        json_files += 1
        top = n.split("/", 1)[0].lower()
        if top == "events":
            events_json += 1
        elif top == "markets":
            markets_json += 1
        elif top == "series":
            series_json += 1
        else:
            unknown_json += 1

    return {
        "total_files": total_files,
        "json_files": json_files,
        "events_json": events_json,
        "markets_json": markets_json,
        "series_json": series_json,
        "unknown_json": unknown_json,
    }


def build_md(owner: str, prefix: str, rows: List[Tuple[str, Dict[str, int], str]]) -> str:
    totals = {k: 0 for k in ["total_files", "json_files", "events_json", "markets_json", "series_json", "unknown_json"]}
    for _, c, _ in rows:
        for k in totals:
            totals[k] += int(c.get(k, 0))

    md: List[str] = []
    md.append("# Kalshi Repo Stats")
    md.append(f"Updated: {utc_now_str()}")
    md.append(f"Owner: {owner}")
    md.append(f"Prefix: {prefix} (auto-detect all repos with this prefix)")
    md.append("")
    md.append("## Summary (All Repos)")
    md.append(f"- Total files: {totals['total_files']:,}")
    md.append(f"- JSON files: {totals['json_files']:,}")
    md.append(f"- event JSON: {totals['events_json']:,}")
    md.append(f"- market JSON: {totals['markets_json']:,}")
    md.append(f"- series JSON: {totals['series_json']:,}")
    md.append(f"- unknown JSON: {totals['unknown_json']:,}")
    md.append("")

    md.append("## Per Repository")
    md.append("Repository | Total files | JSON files | event JSON | market JSON | series JSON | unknown JSON | Note")
    md.append("---|---:|---:|---:|---:|---:|---:|---")
    for name, c, note in rows:
        md.append(
            f"{name} | {c['total_files']:,} | {c['json_files']:,} | {c['events_json']:,} | {c['markets_json']:,} | {c['series_json']:,} | {c['unknown_json']:,} | {note}"
        )

    md.append("")
    md.append("### Notes")
    md.append("- Counts are computed by cloning each repo with `--filter=blob:none --no-checkout --depth 1` and running `git ls-tree`.")
    md.append("- This workflow is intended to run less frequently than the crawl (e.g., daily).")
    return "\n".join(md) + "\n"


def main() -> int:
    owner = os.environ.get("OWNER") or os.environ.get("GITHUB_OWNER") or "statground"
    prefix = os.environ.get("PREFIX") or "Statground_Data_Kalshi"

    repos = gh_list_repos_by_prefix(owner, prefix)
    # Prefer stable ordering: Current, unknown, then years
    def sort_key(r: str):
        if re.search(r"_Current(_\d{3})?$", r):
            return (0, r)
        if re.search(r"_unknown(_\d{3})?$", r):
            return (1, r)
        if r.endswith("_Series"):
            return (0, r)
        m = re.search(r"_(\d{4})$", r)
        if m:
            return (2, int(m.group(1)))
        return (3, r)

    repos = sorted(repos, key=sort_key)

    work = Path("/tmp/kalshi_stats")
    if work.exists():
        subprocess.run(["rm", "-rf", str(work)], check=True)
    work.mkdir(parents=True, exist_ok=True)

    rows: List[Tuple[str, Dict[str, int], str]] = []

    for repo in repos:
        tmp_dir = work / repo
        note = ""
        try:
            counts = count_repo(f"https://github.com/{owner}/{repo}.git", tmp_dir)
        except Exception as e:
            counts = {"total_files": 0, "json_files": 0, "events_json": 0, "markets_json": 0, "series_json": 0, "unknown_json": 0}
            note = f"error: {e}"
        finally:
            subprocess.run(["rm", "-rf", str(tmp_dir)], check=False)

        rows.append((repo, counts, note))

    md = build_md(owner, prefix, rows)
    Path(STATS_MD_FILENAME).write_text(md, encoding="utf-8")

    print(f"Wrote {STATS_MD_FILENAME} ({len(rows)} repos)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())