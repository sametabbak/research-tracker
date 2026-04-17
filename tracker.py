"""
tracker.py
Main entry point. Loads active centers, dispatches to the right fetcher,
diffs against snapshots, notifies on changes.

Usage:
    python tracker.py                  # run all active centers
    python tracker.py aku_tuam         # run a single center by ID
    python tracker.py --check          # connectivity check only, no scraping
"""

import json
import socket
import sys
from pathlib import Path
from urllib.parse import urlparse

from diff_engine import diff, save_snapshot
from notifier import notify, notify_manual, notify_error
from json_exporter import export_center

from fetchers import html_table, html_then_pdf, manual, taum, barum, cutam, bitam, daytam, ikcu_merlab

FETCHERS = {
    "html_table":    html_table.fetch,
    "html_then_pdf": html_then_pdf.fetch,
    "manual":        manual.fetch,
    # "html_dynamic": html_dynamic.fetch,  # coming later (ODTÜ MERLAB)

    # Center-specific fetchers
    "taum":          taum.fetch,
    "barum":         barum.fetch,
    "cutam":         cutam.fetch,
    "bitam":         bitam.fetch,
    "daytam":        daytam.fetch,
    "ikcu_merlab":   ikcu_merlab.fetch,
}

CONFIG_PATH    = Path(__file__).parent / "config" / "centers.json"
PROBE_TIMEOUT  = 15  # generous timeout for the pre-check probe


# ── Connectivity probe ────────────────────────────────────────────────────────

def probe(center: dict) -> bool:
    """Return True if the center's host accepts a TCP connection."""
    if center.get("fetch_method") == "manual":
        return True
    url  = center.get("pricing_url") or center.get("url", "")
    host = urlparse(url).hostname
    if not host:
        return True
    try:
        sock = socket.create_connection((host, 443), timeout=PROBE_TIMEOUT)
        sock.close()
        return True
    except OSError:
        return False


def print_connectivity_report(centers: list[dict]) -> None:
    """--check mode: probe every center and report, then exit."""
    print("🔌 Connectivity check\n")
    all_ok = True
    for c in centers:
        url = c.get("pricing_url") or c.get("url", "")
        ok  = probe(c)
        icon = "✅" if ok else "❌"
        print(f"  {icon} {c['name']:<22} {url}")
        if not ok:
            all_ok = False
            print("       → Unreachable from this network.")
            print("         GitHub Actions (Azure) will likely reach it fine.\n")
    print()
    if all_ok:
        print(f"  All {len(centers)} center(s) reachable. Safe to run locally.")
    else:
        print(
            "  ⚠️  One or more centers unreachable locally.\n"
            "     Push to GitHub and use Actions → 'Run workflow' instead."
        )


# ── Centers loader ────────────────────────────────────────────────────────────

def load_centers(target_id: str | None = None) -> list[dict]:
    centers = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if target_id:
        return [c for c in centers if c["id"] == target_id]
    return [c for c in centers if c.get("active", False)]


# ── Per-center runner ─────────────────────────────────────────────────────────

def run(center: dict) -> None:
    method   = center["fetch_method"]
    fetch_fn = FETCHERS.get(method)

    if fetch_fn is None:
        print(f"[{center['name']}] ⏭️  Skipping — fetcher '{method}' not implemented yet.")
        return

    try:
        result = fetch_fn(center)
    except Exception as e:
        notify_error(center, e)
        return

    if result.get("manual"):
        notify_manual(center)
        return

    diff_report = diff(center["id"], result)
    notify(center, diff_report)
    save_snapshot(result)
    export_center(center, result, diff_report)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    args       = sys.argv[1:]
    check_only = "--check" in args
    target_id  = next((a for a in args if not a.startswith("--")), None)

    centers = load_centers(target_id)

    if not centers:
        print(f"No active centers found{f' matching ID: {target_id}' if target_id else ''}.")
        return

    if check_only:
        print_connectivity_report(centers)
        return

    # Warn about unreachable centers but ALWAYS attempt every center.
    # The fetcher has its own 25-second timeout + retry logic.
    # A failed TCP probe (slow server, Azure routing) should not skip a center.
    for c in centers:
        if c.get("fetch_method") != "manual" and not probe(c):
            print(
                f"  ⚠️  [{c['name']}] Host unreachable in pre-check "
                f"({PROBE_TIMEOUT}s). Attempting anyway...\n"
            )

    print(f"Running tracker for {len(centers)} center(s)...\n")
    for center in centers:
        run(center)


if __name__ == "__main__":
    main()
