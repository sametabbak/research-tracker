"""
json_exporter.py
After a successful fetch + diff, writes structured JSON files the app reads.

Key improvements over v1:
  - Supports explicit column_map in centers.json to override auto-detection
  - Supports column_name_map to find columns by header text (Turkish-safe)
  - Better price parsing (handles ranges, hourly rates, Turkish formatting)
  - Category inference from section headers when no category column exists
  - Skips known noise rows (page numbers, footers, empty cells)
  - Generates data/keywords.json for cross-center compare feature
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

    if not analyses:
        _print_table_diagnostics(center, result)

    _write_analyses(center, analyses)
    _update_history(center, diff_report, analyses)
    _rebuild_centers_index()
    _rebuild_keywords()


def _print_table_diagnostics(center: dict, result: dict) -> None:
    """
    Called when 0 analyses were parsed. Prints the raw table structure
    so you can identify the correct column indices and update column_map
    in centers.json.
    """
    tables = result.get("tables", [])
    name   = center["name"]

    if not tables:
        print(f"  ⚠️  [{name}] No tables found in page. Possible causes:")
        print(f"       - Page requires JavaScript (dynamic rendering)")
        print(f"       - CSS selector '{center.get('selector', 'table')}' matched nothing")
        print(f"       - Page structure changed since last audit")
        return

    print(f"  ⚠️  [{name}] {len(tables)} table(s) found but 0 analyses parsed.")
    print(f"       Showing first 5 rows of each table to help fix column_map:\n")

    for t_idx, table in enumerate(tables):
        print(f"       ── Table {t_idx} ({len(table)} rows) ──")
        for r_idx, row in enumerate(table[:5]):
            cols = " | ".join(f"[{i}] {str(v)[:30]:<30}" for i, v in enumerate(row))
            print(f"       Row {r_idx}: {cols}")
        if len(table) > 5:
            print(f"       ... ({len(table) - 5} more rows)")
        print()

    print(
        f"       ➡  Update 'column_map' in config/centers.json for '{center['id']}':\n"
        f"          Set 'name' to the column index containing the analysis name,\n"
        f"          and 'price' to the column containing the price (TL amount).\n"
        f"          Set other keys to null if that column doesn't exist.\n"
    )


# ── Analysis parsing ──────────────────────────────────────────────────────────

def _parse_analyses(center: dict, result: dict) -> list[dict]:
    """
    Convert raw table rows into structured Analysis objects.

    Priority:
    1. column_name_map — find columns by header text (Turkish-safe)
    2. column_map      — find columns by numeric index
    3. auto-detect     — scan header row for Turkish keywords
    """
    column_map: dict | None = center.get("column_map")
    analyses:   list[dict]  = []
    seen:       set[str]    = set()

    for table in result.get("tables", []):
        if len(table) < 2:
            continue

        # Determine column indices
        if center.get("column_name_map"):
            cols = _resolve_columns_by_name(table[0], center["column_name_map"])
        elif column_map:
            cols = column_map
        else:
            cols = _detect_columns(table[0])

        name_col  = cols.get("name",     0)
        price_col = cols.get("price",    1)
        cat_col   = cols.get("category")
        unit_col  = cols.get("unit")
        note_col  = cols.get("notes")

        current_category = ""

        start_row = 1 if _is_header_row(table[0]) else 0
        for row in table[start_row:]:
            while len(row) <= max(name_col, price_col):
                row.append("")

            name      = row[name_col].strip()  if name_col  < len(row) else ""
            price_raw = row[price_col].strip() if price_col < len(row) else ""

            if _is_section_header(row, price_col):
                current_category = name
                continue

            if _is_noise(name):
                continue

            price = _parse_price(price_raw)
            if not name or price is None:
                continue

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


def _tr_lower(s: str) -> str:
    """Turkish-safe lowercase: handles İ→i and I→ı correctly."""
    return s.replace("İ", "i").replace("I", "ı").lower()


def _resolve_columns_by_name(header_row: list[str], column_name_map: dict) -> dict:
    """
    Find column indices by matching header cell text.
    column_name_map values are substrings to search for in the header (case-insensitive).
    Uses Turkish-aware lowercasing so "İşlem Türü" matches "işlem türü".
    Columns not found fall back to auto-detection.
    """
    auto = _detect_columns(header_row)
    resolved = dict(auto)

    for field, search_text in column_name_map.items():
        if search_text is None:
            resolved[field] = None
            continue
        search_lower = _tr_lower(search_text)
        for i, cell in enumerate(header_row):
            if search_lower in _tr_lower(cell):
                resolved[field] = i
                break

    return resolved


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

    mapping.setdefault("name",  0)
    mapping.setdefault("price", 1)
    return mapping


def _is_header_row(row: list[str]) -> bool:
    text = " ".join(row).lower()
    return any(k in text for k in ["analiz", "ücret", "fiyat", "bedel", "hizmet"])


def _is_section_header(row: list[str], price_col: int) -> bool:
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
    if not raw:
        return None

    cleaned = re.sub(r"[₺TLtl€$\s]", "", raw)
    cleaned = re.sub(r"\+?kdv.*$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\(.*?\)", "", cleaned)
    cleaned = cleaned.strip()

    if not cleaned:
        return None

  if "/" in raw:
        raw = raw.split("/")[0]

    if "-" in cleaned:
        cleaned = cleaned.split("-")[0].strip()

    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    elif re.match(r"^\d{1,3}\.\d{3}$", cleaned):
        cleaned = cleaned.replace(".", "")

    try:
        value = float(cleaned)
        return value if value > 0 else None
    except ValueError:
        return None


# ── File writers ──────────────────────────────────────────────────────────────

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
    """
    Writes data/centers.json — the file the mobile app reads.
    Only includes active centers.
    """
    config_path = Path(__file__).parent / "config" / "centers.json"
    centers_cfg = json.loads(config_path.read_text(encoding="utf-8"))

    index = []
    for c in centers_cfg:
        if not c.get("active", False):
            continue

        analyses_path  = DATA_DIR / "analyses" / f"{c['id']}.json"
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
            "active":         True,
            "reference":      c.get("reference", False),
            "analysis_count": analysis_count,
            "last_updated":   last_updated,
        })

    out = DATA_DIR / "centers.json"
    out.write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"📋 Merkez indeksi güncellendi ({len(index)} aktif merkez) → {out}")


def _rebuild_keywords() -> None:
    """
    Builds data/keywords.json — maps canonical analysis names to keyword aliases.
    The app uses this to group same-analysis results from different centers
    even when names differ (e.g. "XRD", "XRD Analizi", "X-Işını Kırınımı").
    """
    analyses_dir = DATA_DIR / "analyses"
    if not analyses_dir.exists():
        return

    all_names: list[str] = []
    for f in analyses_dir.glob("*.json"):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            all_names.extend(a["name"] for a in data.get("analyses", []))
        except Exception:
            continue

    CANONICAL_KEYWORDS: dict[str, list[str]] = {
        "XRD":            ["xrd", "x-ray", "x ışını", "kırınım", "difraksiyon"],
        "SEM":            ["sem", "taramalı elektron", "scanning electron"],
        "SEM-EDX":        ["edx", "eds", "enerji dağılımlı", "energy dispersive"],
        "TEM":            ["tem", "transmisyon elektron", "transmission electron"],
        "AFM":            ["afm", "atomik kuvvet", "atomic force"],
        "BET":            ["bet", "yüzey alan", "surface area"],
        "TGA":            ["tga", "termogravimetri", "thermogravimetric", "termal gravimetri"],
        "DSC":            ["dsc", "diferansiyel taramalı kalorimetre", "differential scanning"],
        "DTA":            ["dta", "diferansiyel termal analiz"],
        "FTIR":           ["ftir", "ft-ir", "kızılötesi spektroskopi", "infrared"],
        "Raman":          ["raman"],
        "ICP-MS":         ["icp-ms", "icp ms", "kütle spektrometri"],
        "ICP-OES":        ["icp-oes", "icp oes", "optik emisyon"],
        "XRF":            ["xrf", "x-ışını floresans", "x-ray fluorescence"],
        "Porozimetre":    ["porozimetre", "porozimetri", "cıvalı", "mercury porosimetry"],
        "Mikrosertlik":   ["mikrosertlik", "vickers", "hardness"],
        "OES":            ["oes", "optik emisyon spektroskopi"],
        "Eleman Analizi": ["eleman analiz", "elementel", "elemental", "chns"],
        "NMR":            ["nmr", "nükleer manyetik rezonans"],
    }

    keywords_map: dict[str, list[str]] = {}
    for canonical, kws in CANONICAL_KEYWORDS.items():
        matched = []
        for name in all_names:
            name_lower = name.lower()
            if any(kw in name_lower for kw in kws):
                if name not in matched:
                    matched.append(name)
        if matched:
            keywords_map[canonical] = matched

    out = DATA_DIR / "keywords.json"
    out.write_text(json.dumps(keywords_map, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"🔑 Anahtar kelime indeksi güncellendi ({len(keywords_map)} analiz türü) → {out}")
