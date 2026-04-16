"""
tracker.py
Main entry point. Loads active centers, dispatches to the right fetcher,
diffs against snapshots, notifies on changes.

Usage:
    python tracker.py               # run all active centers
    python tracker.py dpu_iltem     # run a single center by ID
"""

import json
import sys
from pathlib import Path

from diff_engine import diff, save_snapshot
from notifier import notify, notify_manual, notify_error
from json_exporter import export_center

# --- Fetcher registry -------------------------------------------
# Add new fetchers here as you implement them
from fetchers import html_table, html_then_pdf, manual

FETCHERS = {
    "html_table":    html_table.fetch,
    "html_then_pdf": html_then_pdf.fetch,
    "manual":        manual.fetch,
    # "html_dynamic": html_dynamic.fetch,  # coming later (ODTÜ MERLAB)
}

# ----------------------------------------------------------------

CONFIG_PATH = Path(__file__).parent / "config" / "centers.json"


def load_centers(target_id: str | None = None) -> list[dict]:
    centers = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if target_id:
        return [c for c in centers if c["id"] == target_id]
    return [c for c in centers if c.get("active", False)]


def run(center: dict) -> None:
    method = center["fetch_method"]
    fetch_fn = FETCHERS.get(method)

    if fetch_fn is None:
        print(f"[{center['name']}] ⏭️  Skipping — fetcher '{method}' not implemented yet.")
        return

    try:
        result = fetch_fn(center)
    except Exception as e:
        notify_error(center, e)
        return

    # Manual centers just need a reminder, no diff
    if result.get("manual"):
        notify_manual(center)
        return

    diff_report = diff(center["id"], result)
    notify(center, diff_report)

    # Always save the latest snapshot (updates the timestamp even if unchanged)
    save_snapshot(result)

    # Export structured JSON for the mobile app
    export_center(center, result, diff_report)


def main() -> None:
    target_id = sys.argv[1] if len(sys.argv) > 1 else None
    centers = load_centers(target_id)

    if not centers:
        print(f"No active centers found{f' matching ID: {target_id}' if target_id else ''}.")
        return

    print(f"Running tracker for {len(centers)} center(s)...\n")
    for center in centers:
        run(center)


if __name__ == "__main__":
    main()
