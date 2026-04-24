"""
sanitize.py
Cleans special characters from analysis names in:
  - data/analyses/*.json  (each analysis name)
  - data/keywords.json    (each alias string)

Run automatically by .github/workflows/sanitize.yml after tracker.yml,
or manually: python sanitize.py

Characters replaced:
  +  →  -   (breaks Shell URL decoding as %2B)
  &  →  ,   (breaks Shell URL decoding as %26)
  #  →  ''  (Shell treats as URL fragment separator)
  ?  →  ''  (Shell treats as query string start)
  =  →  -   (can appear in chemical/lab notation)
"""

import json
import re
from pathlib import Path

DATA_DIR     = Path(__file__).parent / "data"
ANALYSES_DIR = DATA_DIR / "analyses"
KEYWORDS_PATH = DATA_DIR / "keywords.json"

# Characters that cause problems in Shell URL routing / SQLite LIKE
# Ordered: multi-char sequences first (if any), then single chars
REPLACEMENTS: list[tuple[str, str]] = [
    ("+",  "-"),
    ("&",  ","),
    ("#",  ""),
    ("?",  ""),
    ("=",  "-"),
]


def _sanitize(name: str) -> str:
    """Apply all replacements to a single string."""
    for char, replacement in REPLACEMENTS:
        name = name.replace(char, replacement)
    # Collapse multiple spaces that replacements may create
    name = re.sub(r"  +", " ", name)
    return name.strip()


def _clean_analyses() -> int:
    """
    Clean names in every data/analyses/*.json file.
    Returns total number of names changed.
    """
    if not ANALYSES_DIR.exists():
        print("  data/analyses/ not found — skipping.")
        return 0

    total_changed = 0

    for path in sorted(ANALYSES_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  ⚠️  Could not read {path.name}: {e}")
            continue

        analyses  = data.get("analyses", [])
        changed   = 0

        for analysis in analyses:
            original = analysis.get("name", "")
            cleaned  = _sanitize(original)
            if cleaned != original:
                analysis["name"] = cleaned
                changed += 1

        if changed:
            path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            print(f"  ✅ {path.name}: {changed} name(s) cleaned.")
            total_changed += changed
        else:
            print(f"  —  {path.name}: no changes.")

    return total_changed


def _clean_keywords() -> int:
    """
    Clean alias strings in data/keywords.json.
    Returns total number of aliases changed.
    """
    if not KEYWORDS_PATH.exists():
        print("  data/keywords.json not found — skipping.")
        return 0

    try:
        keywords = json.loads(KEYWORDS_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"  ⚠️  Could not read keywords.json: {e}")
        return 0

    total_changed = 0

    for canonical, aliases in keywords.items():
        cleaned_aliases = []
        for alias in aliases:
            cleaned = _sanitize(alias)
            if cleaned != alias:
                total_changed += 1
            cleaned_aliases.append(cleaned)
        keywords[canonical] = cleaned_aliases

    KEYWORDS_PATH.write_text(
        json.dumps(keywords, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    if total_changed:
        print(f"  ✅ keywords.json: {total_changed} alias(es) cleaned.")
    else:
        print(f"  —  keywords.json: no changes.")

    return total_changed


def main() -> None:
    print("🧹 Sanitize — özel karakterler temizleniyor...\n")

    print("📂 data/analyses/")
    analyses_changed = _clean_analyses()

    print("\n📂 data/keywords.json")
    keywords_changed = _clean_keywords()

    total = analyses_changed + keywords_changed
    print(f"\n✅ Tamamlandı. Toplam {total} değişiklik yapıldı.")


if __name__ == "__main__":
    main()
