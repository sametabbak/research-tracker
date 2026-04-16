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

from fetchers import html_table, html_then_pdf, manual

FETCHERS = {
    "html_table":    html_table.fetch,
    "html_then_pdf": html_then_pdf.fetch,
    "manual":        manual.fetch,
    # "html_dynamic": html_dynamic.fetch,  # coming later (ODTÜ MERLAB)
}

CONFIG_PATH = Path(__file__).parent / "config" / "centers.json"
CONNECT_TIMEOUT = 6  # seconds for the pre-check probe


# ── Connectivity pre-check ────────────────────────────────────────────────────

def check_connectivity(centers: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Quick TCP probe to each center's host before scraping.
    Returns (reachable, unreachable) lists.
    Skips manual centers — they have no URL to probe.
    """
    reachable   = []
    unreachable = []

    for center in centers:
        if center.get("fetch_method") == "manual":
            reachable.append(center)
            continue

        url  = center.get("pricing_url") or center.get("url", "")
        host = urlparse(url).hostname
        if not host:
            reachable.append(center)
            continue

        try:
            sock = socket.create_connection((host, 443), timeout=CONNECT_TIMEOUT)
            sock.close()
            reachable.append(center)
        except OSError:
            unreachable.append(center)

    return reachable, unreachable


def print_connectivity_report(centers: list[dict]) -> None:
    """Run --check mode: probe all active centers and report."""
    print("🔌 Connectivity check\n")
    reachable, unreachable = check_connectivity(centers)

    for c in reachable:
        url = c.get("pricing_url") or c.get("url", "")
        print(f"  ✅ {c['name']:<20}  {url}")

    for c in unreachable:
        url = c.get("pricing_url") or c.get("url", "")
        print(f"  ❌ {c['name']:<20}  {url}")
        print(f"       → Cannot reach from this network.")
        print(f"         The GitHub Actions runner will likely reach it fine.")

    print()
    if unreachable:
        print(
            f"  ⚠️  {len(unreachable)} center(s) unreachable locally.\n"
            "     Push to GitHub and use the Actions tab → 'Run workflow'\n"
            "     to run the tracker on GitHub's infrastructure instead."
        )
    else:
        print(f"  All {len(reachable)} center(s) reachable. Safe to run locally.")


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
    args      = sys.argv[1:]
    check_only = "--check" in args
    target_id  = next((a for a in args if not a.startswith("--")), None)

    centers = load_centers(target_id)

    if not centers:
        print(f"No active centers found{f' matching ID: {target_id}' if target_id else ''}.")
        return

    if check_only:
        print_connectivity_report(centers)
        return

    # ── Pre-check: skip unreachable centers rather than wasting retry time ──
    reachable, unreachable = check_connectivity(centers)

    if unreachable:
        print("⚠️  Connectivity pre-check:\n")
        for c in unreachable:
            print(
                f"  ❌ {c['name']} — unreachable from this network. Skipping.\n"
                "     Run via GitHub Actions for reliable access to .edu.tr domains."
            )
        print()

    if not reachable:
        print("No reachable centers. Exiting.")
        return

    print(f"Running tracker for {len(reachable)} center(s)...\n")
    for center in reachable:
        run(center)


if __name__ == "__main__":
    main()
