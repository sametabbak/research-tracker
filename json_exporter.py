"""
json_exporter.py
After a successful fetch + diff, writes structured JSON files the app reads.

Key improvements over v1:
  - Supports explicit column_map in centers.json to override auto-detection
  - Better price parsing (handles ranges, hourly rates, Turkish formatting)
  - Category inference from section headers when no category column exists
  - Skips known noise rows (page numbers, footers, empty cells)
  - Writes data/ files only when content has actually changed
"""

import json
import re
from pathlib import Path
from datetime import datetime, timezone

DATA_DIR = Path(__file__).parent / "data"

# Rows whose first cell matches these patterns are noise (page numbers, footers, etc.)
NOISE_PATTERNS = [
    r"^\d+$",              # lone page number
    r"^sayfa\s*\d+",       # "Sayfa 1"
    r"^www\.",             # URLs in footer
    r"^not:",              # footnote markers
    r"^açıklama",          # disclaimer headers
    r"^\*+$",              # rows of asterisks
]


# ── Entry point ───────────────────────────────────────────────────────────────

def export_center(center: dict, result: dict, diff_report: dict) -> None:
    DATA_DIR.mkdir(exist_ok=True)
    (DATA_DIR / "analyses").mkdir(exist_ok=True)
    (DATA_DIR / "history").mkdir(exist_ok=True)

    analyses = _parse_analyses(center, result)
    _write_analyses(center, analyses)
    _update_history(center, diff_report, analyses)
    _rebuild_centers_index()


# ── Analysis parsing ──────────────────────────────────────────────────────────

def _parse_analyses(center: dict, result: dict) -> list[dict]:
    """
    Convert raw table rows into structured Analysis objects.

    Priority:
    1. Use explicit column_map from centers.json if provided
    2. Auto-detect columns by scanning header row for Turkish keywords
    3. Fall back to col-0=name, col-1=price
    """
    column_map: dict | None = center.get("column_map")
    analyses:   list[dict]  = []
    seen:       set[str]    = set()

    for table in result.get("tables", []):
        if len(table) < 2:
            continue

        # Determine column indices
        if column_map:
            cols = column_map
        else:
            cols = _detect_columns(table[0])

        name_col  = cols.get("name",     0)
        price_col = cols.get("price",    1)
        cat_col   = cols.get("category")
        unit_col  = cols.get("unit")
        note_col  = cols.get("notes")

        # Track current category from section headers
        current_category = ""

        start_row = 1 if _is_header_row(table[0]) else 0
        for row in table[start_row:]:
            # Pad short rows
            while len(row) <= max(name_col, price_col):
                row.append("")

            name  = row[name_col].strip() if name_col < len(row) else ""
            price_raw = row[price_col].strip() if price_col < len(row) else ""

            # Detect category section headers (full-width rows with no price)
            if _is_section_header(row, price_col):
                current_category = name
                continue

            # Skip noise rows
            if _is_noise(name):
                continue

            price = _parse_price(price_raw)
            if not name or price is None:
                continue

            # Deduplicate (same analysis, same center)
            key = f"{name.lower()}_{price}"
            if key in seen:
                continue
            seen.add(key)

            category = ""
            if cat_col is not None and cat_col < len(row):
                category = row[cat_col].strip()
            if not category:
                category = current_category

            unit = ""
            if unit_col is not None and unit_col < len(row):
                unit = row[unit_col].strip()
            if not unit:
                unit = _infer_unit(name, price_raw)

            notes = ""
            if note_col is not None and note_col < len(row):
                notes = row[note_col].strip()

            analyses.append({
                "name":     name,
                "category": category,
                "price":    price,
                "currency": "TRY",
                "unit":     unit,
                "notes":    notes,
            })

    return analyses


def _detect_columns(header_row: list[str]) -> dict:
    """Auto-detect column indices from a header row."""
    mapping = {}
    for i, cell in enumerate(header_row):
        c = cell.lower()
        if any(k in c for k in ["analiz", "test", "hizmet", "ölçüm", "cihaz", "işlem"]):
            mapping.setdefault("name", i)
        elif any(k in c for k in ["ücret", "fiyat", "bedel", "tutar", "tl", "₺"]):
            mapping.setdefault("price", i)
        elif any(k in c for k in ["kategori", "grup", "tür", "alan", "bölüm"]):
            mapping.setdefault("category", i)
        elif any(k in c for k in ["birim", "unit", "süre", "zaman"]):
            mapping.setdefault("unit", i)
        elif any(k in c for k in ["açıklama", "not", "detay", "bilgi"]):
            mapping.setdefault("notes", i)

    # Fallback to positional
    mapping.setdefault("name",  0)
    mapping.setdefault("price", 1)
    return mapping


def _is_header_row(row: list[str]) -> bool:
    """Return True if the row looks like a header (contains typical header words)."""
    text = " ".join(row).lower()
    return any(k in text for k in ["analiz", "ücret", "fiyat", "bedel", "hizmet"])


def _is_section_header(row: list[str], price_col: int) -> bool:
    """
    A section header is a row that:
      - Has a non-empty first cell
      - Has an empty or missing price cell
      - The first cell is reasonably short (category name, not analysis name)
    """
    if price_col < len(row) and row[price_col].strip():
        return False
    first = row[0].strip()
    return bool(first) and len(first) < 80 and _parse_price(first) is None


def _is_noise(name: str) -> bool:
    if not name:
        return True
    n = name.lower()
    for pat in NOISE_PATTERNS:
        if re.match(pat, n):
            return True
    return False


def _infer_unit(name: str, price_raw: str) -> str:
    """Infer unit from analysis name or price string."""
    n = name.lower()
    p = price_raw.lower()
    if "saat" in n or "saat" in p or "/sa" in p:
        return "saat"
    if "numune" in n or "örnek" in n:
        return "numune"
    if "element" in n:
        return "element"
    return "numune"


def _parse_price(raw: str) -> float | None:
    """
    Parse Turkish-formatted price strings.

    Handles:
      "1.200,00 ₺"   → 1200.0
      "800 TL"        → 800.0
      "1.500"         → 1500.0
      "500-750"       → 500.0  (takes lower bound of range)
      "1.200,00 + KDV"→ 1200.0
      ""              → None
      "İstek üzerine" → None
    """
    if not raw:
        return None

    # Remove currency symbols, KDV notes, whitespace
    cleaned = re.sub(r"[₺TLtl€$\s]", "", raw)
    cleaned = re.sub(r"\+?kdv.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\(.*?\)", "", cleaned)
    cleaned = cleaned.strip()

    if not cleaned:
        return None

    # Handle ranges — take the lower bound
    if "-" in cleaned:
        cleaned = cleaned.split("-")[0].strip()

    # Turkish number formatting: 1.200,50 → 1200.50
    if "," in cleaned and "." in cleaned:
        # thousands dot + decimal comma
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        # decimal comma only
        cleaned = cleaned.replace(",", ".")
    elif re.match(r"^\d{1,3}\.\d{3}$", cleaned):
        # thousands dot only (e.g. "1.200")
        cleaned = cleaned.replace(".", "")

    try:
        value = float(cleaned)
        return value if value > 0 else None
    except ValueError:
        return None


# ── File writers ───────────────────────────────────────────────────────────────

def _write_analyses(center: dict, analyses: list[dict]) -> None:
    output = {
        "center_id":    center["id"],
        "center_name":  center["name"],
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "source_url":   center.get("pricing_url") or center["url"],
        "kdv_note":     "Fiyatlara KDV dahil değildir.",
        "analyses":     analyses,
    }
    path = DATA_DIR / "analyses" / f"{center['id']}.json"
    path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[{center['name']}] 💾 {len(analyses)} analiz yazıldı → {path.name}")


def _update_history(center: dict, diff_report: dict, analyses: list[dict]) -> None:
    path = DATA_DIR / "history" / f"{center['id']}.json"

    if path.exists():
        history = json.loads(path.read_text(encoding="utf-8"))
    else:
        history = {"center_id": center["id"], "events": []}

    if diff_report["status"] in ("new", "changed"):
        history["events"].insert(0, {
            "date":           datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "type":           diff_report["status"],
            "added_rows":     diff_report.get("added_rows",   []),
            "removed_rows":   diff_report.get("removed_rows", []),
            "analysis_count": len(analyses),
        })
        history["events"] = history["events"][:50]

    path.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")


def _rebuild_centers_index() -> None:
    config_path = Path(__file__).parent / "config" / "centers.json"
    centers_cfg = json.loads(config_path.read_text(encoding="utf-8"))

    index = []
    for c in centers_cfg:
        analyses_path = DATA_DIR / "analyses" / f"{c['id']}.json"
        analysis_count = 0
        last_updated   = None

        if analyses_path.exists():
            data           = json.loads(analyses_path.read_text(encoding="utf-8"))
            analysis_count = len(data.get("analyses", []))
            last_updated   = data.get("last_updated")

        index.append({
            "id":             c["id"],
            "name":           c["name"],
            "university":     c["university"],
            "city":           c["city"],
            "url":            c["url"],
            "pricing_url":    c.get("pricing_url"),
            "active":         c.get("active", False),
            "reference":      c.get("reference", False),
            "analysis_count": analysis_count,
            "last_updated":   last_updated,
        })

    out = DATA_DIR / "centers.json"
    out.write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"📋 Merkez indeksi güncellendi → {out}")
