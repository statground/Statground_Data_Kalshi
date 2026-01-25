#!/usr/bin/env python3
"""Generate a markdown stats file for Kalshi fan-out repos.

Design goals
- Prefer *no* GitHub REST API calls (avoids rate limits).
- Use a repo manifest produced by the crawler: `KALSHI_REPOS.json`.
- Read per-repo counts from the raw content endpoint:
  https://raw.githubusercontent.com/<owner>/<repo>/main/KALSHI_COUNTS.json

If the manifest is missing, we fall back to the GitHub REST API to discover repos.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

import requests


OWNER = os.environ.get("GH_OWNER", "statground")
PREFIX = os.environ.get("KALSHI_REPO_PREFIX", "Statground_Data_Kalshi")
MANIFEST_PATH = os.environ.get("KALSHI_REPO_MANIFEST", "KALSHI_REPOS.json")
COUNTS_NAME = os.environ.get("KALSHI_COUNTS_FILE", "KALSHI_COUNTS.json")
OUT_MD = os.environ.get("KALSHI_STATS_MD", "KALSHI_REPO_STATS.md")

GH_API = "https://api.github.com"


def gh_token() -> Optional[str]:
    return os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")


def gh_headers() -> Dict[str, str]:
    h = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "statground-kalshi-stats",
    }
    tok = gh_token()
    if tok:
        h["Authorization"] = f"token {tok}"
    return h


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def load_manifest() -> Optional[List[str]]:
    if not os.path.exists(MANIFEST_PATH):
        return None
    try:
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        repos = data.get("repos") if isinstance(data, dict) else data
        if not isinstance(repos, list):
            return None
        repos = [r for r in repos if isinstance(r, str)]
        return sorted(set(repos))
    except Exception:
        return None


def list_repos_via_api() -> List[str]:
    """Fallback discovery only (may hit rate limits for large accounts)."""
    repos: List[str] = []
    url = f"{GH_API}/users/{OWNER}/repos"
    params = {"per_page": 100, "type": "owner", "sort": "full_name", "direction": "asc"}

    while url:
        r = requests.get(url, headers=gh_headers(), params=params, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"GitHub API repo list failed: {r.status_code} {r.text[:300]}")
        items = r.json()
        for it in items:
            name = it.get("name")
            if isinstance(name, str) and name.startswith(PREFIX):
                repos.append(name)
        # pagination
        nxt = None
        if "Link" in r.headers:
            # very small parser: find rel="next"
            for part in r.headers["Link"].split(","):
                if 'rel="next"' in part:
                    nxt = part.split(";")[0].strip().lstrip("<").rstrip(">")
        url = nxt
        params = None

    return sorted(set(repos))


@dataclass
class RepoRow:
    repo: str
    total_files: int = 0
    json_files: int = 0
    series_json: int = 0
    event_json: int = 0
    market_json: int = 0
    series_meta: int = 0
    event_meta: int = 0
    market_meta: int = 0
    note: str = ""


def fetch_counts_raw(repo: str) -> Optional[Dict]:
    url = f"https://raw.githubusercontent.com/{OWNER}/{repo}/main/{COUNTS_NAME}"
    r = requests.get(url, timeout=30)
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        raise RuntimeError(f"raw counts fetch failed for {repo}: {r.status_code} {r.text[:200]}")
    return r.json()


def build_rows(repos: List[str]) -> List[RepoRow]:
    rows: List[RepoRow] = []
    for repo in repos:
        row = RepoRow(repo=repo)
        try:
            c = fetch_counts_raw(repo)
            if not c:
                row.note = "counts file missing (will appear after first flush/close)"
                rows.append(row)
                continue
            row.total_files = int(c.get("total_files", 0) or 0)
            row.json_files = int(c.get("json_files", 0) or 0)
            by_kind = c.get("by_kind") or {}
            by_meta = c.get("by_kind_meta") or {}
            row.series_json = int(by_kind.get("series", 0) or 0)
            row.event_json = int(by_kind.get("event", 0) or 0)
            row.market_json = int(by_kind.get("market", 0) or 0)
            row.series_meta = int(by_meta.get("series", 0) or 0)
            row.event_meta = int(by_meta.get("event", 0) or 0)
            row.market_meta = int(by_meta.get("market", 0) or 0)
            rows.append(row)
        except Exception as e:
            row.note = f"error reading counts: {e}"
            rows.append(row)
    return rows


def write_md(rows: List[RepoRow]) -> None:
    total_files = sum(r.total_files for r in rows)
    json_files = sum(r.json_files for r in rows)
    event_json = sum(r.event_json for r in rows)
    market_json = sum(r.market_json for r in rows)
    series_json = sum(r.series_json for r in rows)
    event_meta = sum(r.event_meta for r in rows)
    market_meta = sum(r.market_meta for r in rows)
    series_meta = sum(r.series_meta for r in rows)

    lines: List[str] = []
    lines.append("# Kalshi Repo Stats")
    lines.append(f"Updated: {utc_now_str()}")
    lines.append(f"Owner: {OWNER}")
    lines.append(f"Prefix: {PREFIX} (manifest-first; raw counts)")
    lines.append("")
    lines.append("## Summary (All Repos)")
    lines.append(f"- Total files: {total_files:,}")
    lines.append(f"- JSON files: {json_files:,}")
    lines.append(f"- event JSON: {event_json:,} (excluding meta)")
    lines.append(f"- market JSON: {market_json:,} (excluding meta)")
    lines.append(f"- series JSON: {series_json:,} (excluding meta)")
    lines.append(f"- event meta: {event_meta:,}")
    lines.append(f"- market meta: {market_meta:,}")
    lines.append(f"- series meta: {series_meta:,}")
    lines.append("")

    lines.append("## Per Repository")
    lines.append(
        "Repository | Total files | JSON files | event JSON | market JSON | series JSON | event meta | market meta | series meta | Note"
    )
    lines.append("---|---:|---:|---:|---:|---:|---:|---:|---:|---")

    for r in rows:
        lines.append(
            f"{r.repo} | {r.total_files:,} | {r.json_files:,} | {r.event_json:,} | {r.market_json:,} | {r.series_json:,} | {r.event_meta:,} | {r.market_meta:,} | {r.series_meta:,} | {r.note}"
        )

    lines.append("")
    lines.append("### Notes")
    lines.append(f"- Repos are read from `{MANIFEST_PATH}` when present (written by the crawler).")
    lines.append(f"- Counts are read from `{COUNTS_NAME}` via raw.githubusercontent.com (no GitHub REST API).")
    lines.append("- If the manifest is missing, the script falls back to the GitHub REST API to discover repos.")

    with open(OUT_MD, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def main() -> None:
    repos = load_manifest()
    if repos is None:
        repos = list_repos_via_api()

    rows = build_rows(repos)
    write_md(rows)
    print(f"Wrote {OUT_MD} ({len(rows)} repos)")


if __name__ == "__main__":
    main()
