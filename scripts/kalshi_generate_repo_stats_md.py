#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate KALSHI_REPO_STATS.md by reading KALSHI_COUNTS.json from each Kalshi repo.

This mirrors the Polymarket approach:
- Each data repo updates a `KALSHI_COUNTS.json` during crawling.
- This script lists all repos under OWNER whose names start with `Statground_Data_Kalshi_`
  and aggregates those counts into a markdown report.

Env:
- GH_PAT (or GITHUB_TOKEN) required
- GITHUB_OWNER (default: statground)
- PREFIX (default: Statground_Data_Kalshi_)
- OUT_MD (default: KALSHI_REPO_STATS.md)
"""

from __future__ import annotations

import os
import json
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests

GH_PAT = os.environ.get("GH_PAT") or os.environ.get("GITHUB_TOKEN")
if not GH_PAT:
    raise RuntimeError("GH_PAT (or GITHUB_TOKEN) is required")

OWNER = os.environ.get("GITHUB_OWNER", "statground").strip()
PREFIX = os.environ.get("PREFIX", "Statground_Data_Kalshi_").strip()
OUT_MD = os.environ.get("OUT_MD", "KALSHI_REPO_STATS.md").strip()

UA = "statground-kalshi-stats/1.0"
GH_API = "https://api.github.com"

def gh_headers() -> Dict[str, str]:
    return {
        "Authorization": f"token {GH_PAT}",
        "Accept": "application/vnd.github+json",
        "User-Agent": UA,
    }

def list_repos(owner: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    page = 1
    while True:
        r = requests.get(
            f"{GH_API}/users/{owner}/repos",
            headers=gh_headers(),
            params={"per_page": 100, "page": page, "sort": "full_name"},
            timeout=60,
        )
        r.raise_for_status()
        batch = r.json() or []
        if not batch:
            break
        out.extend(batch)
        page += 1
    return out

def get_counts(owner: str, repo: str) -> Optional[Dict[str, Any]]:
    # Use contents API to fetch KALSHI_COUNTS.json from default branch
    r = requests.get(
        f"{GH_API}/repos/{owner}/{repo}/contents/KALSHI_COUNTS.json",
        headers=gh_headers(),
        timeout=60,
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    data = r.json()
    # contents API returns base64 content
    import base64
    content = base64.b64decode(data["content"]).decode("utf-8")
    return json.loads(content)

def md_escape(s: str) -> str:
    return s.replace("|", "\\|")

def main() -> None:
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    repos = [r for r in list_repos(OWNER) if (r.get("name") or "").startswith(PREFIX)]
    repos.sort(key=lambda x: x.get("name") or "")

    rows: List[Tuple[str, Dict[str, Any], str]] = []
    total = {
        "total_files": 0, "json_files": 0,
        "series_json": 0, "event_json": 0, "market_json": 0,
    }

    for r in repos:
        name = r.get("name") or ""
        counts = get_counts(OWNER, name)
        note = ""
        if not counts:
            counts = {"total_files": 0, "json_files": 0, "series_json": 0, "event_json": 0, "market_json": 0}
            note = "counts file missing"
        for k in total:
            total[k] += int(counts.get(k, 0) or 0)
        rows.append((name, counts, note))

    lines: List[str] = []
    lines.append("# Kalshi Repo Stats")
    lines.append(f"Updated: {now}")
    lines.append(f"Owner: {OWNER}")
    lines.append(f"Prefix: {PREFIX} (auto-detect repos)")
    lines.append("")
    lines.append("## Summary (All Repos)")
    lines.append(f"- Total files: {total['total_files']:,}")
    lines.append(f"- JSON files: {total['json_files']:,}")
    lines.append(f"- series JSON: {total['series_json']:,}")
    lines.append(f"- event JSON: {total['event_json']:,}")
    lines.append(f"- market JSON: {total['market_json']:,}")
    lines.append("")
    lines.append("## Per Repository")
    lines.append("Repository | Total files | JSON files | series JSON | event JSON | market JSON | Note")
    lines.append("---|---:|---:|---:|---:|---:|---")
    for name, c, note in rows:
        lines.append(
            f"{md_escape(name)} | {int(c.get('total_files',0)):,} | {int(c.get('json_files',0)):,} | "
            f"{int(c.get('series_json',0)):,} | {int(c.get('event_json',0)):,} | {int(c.get('market_json',0)):,} | {md_escape(note)}"
        )
    lines.append("")
    lines.append("### Notes")
    lines.append("- Counts are read from `KALSHI_COUNTS.json` in each repo.")
    lines.append("- That file is updated during the crawl (on each flush).")
    lines.append("- This stats script can run frequently; the timestamp updates even if counts do not change.")

    Path(OUT_MD).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {OUT_MD} ({len(repos)} repos)")

if __name__ == "__main__":
    main()
