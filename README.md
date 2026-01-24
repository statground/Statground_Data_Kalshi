# Statground_Data_Kalshi (Orchestrator) — Auto-create edition

This repo runs GitHub Actions to crawl Kalshi and fan-out JSON into separate repos.
**Target repos are auto-created if missing.**

## Required setup
1) Add `GH_PAT` secret in this orchestrator repo (fine-grained PAT, **All repositories** recommended).
2) Settings → Actions → General → Workflow permissions → **Read and write**.

## Targets
Edit `.state/kalshi_targets.json` (optional). If missing, defaults are generated.

Default:
- Current: `Statground_Data_Kalshi_Current`
- Years: `Statground_Data_Kalshi_<YEAR>`

The script will create missing repos under the configured owner (org/user).

## First run vs incremental
- First run: FULL (series + events + markets)
- Subsequent runs: incremental (markets via min_updated_ts; events via open+min_close_ts backfill)

## Outputs
- Data repos will have `series/`, `events/`, `markets/` and `KALSHI_COUNTS.json`.
- Orchestrator commits `.state/kalshi_state.json`, `.state/kalshi_targets.json`, `manifest.json`, `KALSHI_REPO_STATS.md`.
