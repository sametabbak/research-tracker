"""
migrate_keyword_groups.py
─────────────────────────
One-time migration script.

Reads the last-known-good keywords.json and stamps a `keyword_group` field
onto every matching analysis entry in data/analyses/*.json.

Run once from the repo root:
    python migrate_keyword_groups.py

After this, auto_categorizer.py and json_exporter.py take over maintaining
the field going forward. This script is safe to re-run — it never overwrites
an already-set keyword_group.
"""

import json
import glob
from pathlib import Path

KEYWORDS_FILE  = Path("data/keywords.json")
ANALYSES_GLOB  = "data/analyses/*.json"
FALLBACK_GROUP = "Diğer Analiz ve Laboratuvar Hizmetleri"


def build_reverse_index(kw: dict) -> dict[str, str]:
    """
    Build name → group mapping from keywords.json.
    Excludes the Diğer fallback group — those entries have no real group.
    When a name appears in more than one group, the first non-Diğer wins.
    """
    reverse: dict[str, str] = {}
    for group, names in kw.items():
        if group == FALLBACK_GROUP:
            continue
        for name in names:
            clean = name.strip()
            if clean and clean not in reverse:
                reverse[clean] = group
    return reverse


def migrate() -> None:
    if not KEYWORDS_FILE.exists():
        print("data/keywords.json not found.")
        return

    kw      = json.loads(KEYWORDS_FILE.read_text(encoding="utf-8"))
    reverse = build_reverse_index(kw)
    print(f"Reverse index built: {len(reverse)} name → group mappings\n")

    total_files    = 0
    total_stamped  = 0
    total_skipped  = 0
    total_no_match = 0

    for path in sorted(glob.glob(ANALYSES_GLOB)):
        p = Path(path)
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  ⚠️  Cannot read {p.name}: {e}")
            continue

        analyses = data.get("analyses", [])
        changed  = 0

        for entry in analyses:
            name = entry.get("name", "").strip()
            if not name:
                continue

            existing = entry.get("keyword_group", "")

            # Never overwrite an existing assignment
            if existing:
                total_skipped += 1
                continue

            group = reverse.get(name)
            if group:
                entry["keyword_group"] = group
                changed += 1
                total_stamped += 1
            else:
                # Leave keyword_group absent / null — panel will show as ungrouped
                entry["keyword_group"] = None
                total_no_match += 1

        if changed:
            p.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            print(f"  ✅ {p.name}: {changed} analyses stamped with keyword_group")
        else:
            print(f"  —  {p.name}: no new stamps needed")

        total_files += 1

    print(f"\nDone.")
    print(f"  Files processed : {total_files}")
    print(f"  Stamped          : {total_stamped}")
    print(f"  Already had group: {total_skipped}")
    print(f"  No match found   : {total_no_match}  ← these need manual grouping via panel")


if __name__ == "__main__":
    migrate()
