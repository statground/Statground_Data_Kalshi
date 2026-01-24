# --- PATCHED FILE ---
# kalshi_crawl_and_fanout.py
# Fix: define SERIES_REPO and keep series in a dedicated repository

from pathlib import Path
import logging
import subprocess
import sys
from datetime import datetime, timezone

log = logging.getLogger(__name__)

OWNER = "statground"

CURRENT_REPO = "Statground_Data_Kalshi_Current"
UNKNOWN_REPO = "Statground_Data_Kalshi_unknown"
SERIES_REPO = "Statground_Data_Kalshi_Series"  # <-- FIXED: defined explicitly

NOW_UTC = datetime.now(timezone.utc)

def target_repo_for_relpath(relpath: Path, targets: dict[str, str]) -> str:
    # Route series files to a dedicated repository
    if relpath.parts and relpath.parts[0] == "series":
        return SERIES_REPO
    return targets.get(relpath.parts[0], UNKNOWN_REPO)

# --- NOTE ---
# This file only includes the minimal patched section required to fix the NameError.
# Apply this patch by replacing the corresponding constants and function
# in your existing kalshi_crawl_and_fanout.py.
#
# No other logic has been modified.
