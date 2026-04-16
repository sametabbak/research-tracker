"""
Diff engine
Compares a newly fetched result against the stored snapshot.
Reports what changed: new rows, removed rows, modified prices.
"""

import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone


SNAPSHOTS_DIR = Path(__file__).parent / "snapshots"


def snapshot_path(center_id: str) -> Path:
    return SNAPSHOTS_DIR / f"{center_id}.json"


def load_snapshot(center_id: str) -> dict | None:
    path = snapshot_path(center_id)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return None


def save_snapshot(result: dict) -> None:
    SNAPSHOTS_DIR.mkdir(exist_ok=True)
    center_id = result["center_id"]
    snapshot = {
        **result,
        "snapshot_time": datetime.now(timezone.utc).isoformat(),
        "content_hash": _hash(result["raw_text"]),
    }
    snapshot_path(center_id).write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )


def diff(center_id: str, new_result: dict) -> dict:
    """
    Compare new_result against the stored snapshot.

    Returns a diff report:
    {
        "center_id": str,
        "status": "new" | "changed" | "unchanged",
        "hash_changed": bool,
        "pdf_url_changed": bool,   # only for html_then_pdf centers
        "added_rows": [...],
        "removed_rows": [...],
        "modified_rows": [...],    # [ (old_row, new_row), ... ]
        "snapshot_time": str,
        "previous_snapshot_time": str | None
    }
    """
    old = load_snapshot(center_id)
    new_hash = _hash(new_result["raw_text"])
    now = datetime.now(timezone.utc).isoformat()

    if old is None:
        return {
            "center_id": center_id,
            "status": "new",
            "hash_changed": True,
            "pdf_url_changed": False,
            "added_rows": _all_rows(new_result),
            "removed_rows": [],
            "modified_rows": [],
            "snapshot_time": now,
            "previous_snapshot_time": None,
        }

    hash_changed = old.get("content_hash") != new_hash
    pdf_url_changed = (
        old.get("pdf_url") != new_result.get("pdf_url")
        and new_result.get("pdf_url") is not None
    )

    if not hash_changed and not pdf_url_changed:
        return {
            "center_id": center_id,
            "status": "unchanged",
            "hash_changed": False,
            "pdf_url_changed": False,
            "added_rows": [],
            "removed_rows": [],
            "modified_rows": [],
            "snapshot_time": now,
            "previous_snapshot_time": old.get("snapshot_time"),
        }

    # Content changed — do a row-level diff across all tables
    old_rows = set(tuple(r) for r in _all_rows(old))
    new_rows = set(tuple(r) for r in _all_rows(new_result))

    added   = [list(r) for r in new_rows - old_rows]
    removed = [list(r) for r in old_rows - new_rows]

    return {
        "center_id": center_id,
        "status": "changed",
        "hash_changed": hash_changed,
        "pdf_url_changed": pdf_url_changed,
        "added_rows": added,
        "removed_rows": removed,
        "modified_rows": [],   # future: smarter row matching by analysis name
        "snapshot_time": now,
        "previous_snapshot_time": old.get("snapshot_time"),
    }


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _all_rows(result: dict) -> list[list]:
    """Flatten all tables in a result into a single list of rows."""
    rows = []
    for table in result.get("tables", []):
        rows.extend(table)
    return rows
