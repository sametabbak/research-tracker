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
        # Priority: column_name_map (by header text) > column_map (by index) > auto-detect
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

    # If tables yielded nothing and raw_text is available, try text-based parsing
    if not analyses and result.get("raw_text") and center.get("pdf_text_fallback"):
        analyses = _parse_pdf_text(result["raw_text"])

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



def _parse_pdf_text(raw_text: str) -> list[dict]:
    """
    Fallback text-based parser for bilingual PDFs where pdfplumber table
    extraction produces fragmented or misaligned columns (e.g. DAYTAM).

    Strategy:
    - Scan each line for an analysis code  (e.g. TEM-01*, AFM-03, XRD-02**)
    - Find the associated price in the same or next 3 lines
    - Extract the Turkish analysis name from surrounding text
    - Infer category from all-caps section headers

    Enable by setting  "pdf_text_fallback": true  in centers.json.
    The fetcher must use  html_then_pdf  so raw_text is available in result.
    """
    if not raw_text:
        return []

    # Code pattern: 2-12 uppercase/Turkish chars, dash, 2 digits, 0-3 asterisks
    # (?=\s|[,(]|$) instead of \b so asterisks are included in the match
    CODE_RE  = re.compile(r'(?<![\w*])([A-ZÇĞİÖŞÜa-z]{2,12}-\d{2}[*]{0,3})(?=\s|[,(]|$)')
    PRICE_RE = re.compile(r'\b(\d{1,3}(?:\.\d{3})*,\d{2})\b')
    UNIT_STRIP = re.compile(
        r'\s*(Adet|Piece|Saat|Hour|Gün|Day|Set|Vial|Flask|Jel|Gel|Tüp|Tube|'
        r'Örnek|Sample|Mikroplaka|Microplate|Membran|Membrane|Günlük|Daily)\s*$',
        re.IGNORECASE)

    lines = [l.strip() for l in raw_text.split("\n")]
    analyses: list[dict] = []
    seen:     set[str]   = set()
    current_category     = ""

    for i, line in enumerate(lines):
        if not line:
            continue

        # Section header: all uppercase, short, no code or price
        if (re.match(r"^[A-ZÇĞİÖŞÜ\s\-–/()]{8,}$", line)
                and not CODE_RE.search(line)
                and not PRICE_RE.search(line)
                and len(line.split()) <= 8):
            current_category = line.title()
            continue

        code_m = CODE_RE.search(line)
        if not code_m:
            continue

        # Find price within next 3 lines
        price = None
        for offset in range(0, 4):
            idx = i + offset
            if idx >= len(lines):
                break
            pm = PRICE_RE.search(lines[idx])
            if pm:
                price = _parse_price(pm.group(1))
                break

        if not price:
            continue  # "Daytamla Görüşülecek" or similar — skip

        # Extract name from surrounding text
        after  = PRICE_RE.sub("", line[code_m.end():]).strip()
        after  = UNIT_STRIP.sub("", after).strip()
        before = UNIT_STRIP.sub("", line[:code_m.start()].strip()).strip()

        prev_name = ""
        for j in range(i - 1, max(i - 3, -1), -1):
            prev = lines[j]
            if (prev
                    and not CODE_RE.search(prev)
                    and not PRICE_RE.search(prev)
                    and not re.match(r"^[A-Z\s\-–/()*]{10,}$", prev)):
                cleaned = UNIT_STRIP.sub("", prev).strip()
                if len(cleaned) > 4 and not re.match(r"^[\*\d]", cleaned):
                    prev_name = cleaned
                    break

        name = after if len(after) > 4 else (before if len(before) > 4 else prev_name)

        if not name or len(name) < 4 or re.match(r"^[\*\d]", name):
            continue

        key = f"{name.lower()}_{price}"
        if key in seen:
            continue
        seen.add(key)

        analyses.append({
            "name":     name,
            "category": current_category,
            "price":    price,
            "currency": "TRY",
            "unit":     _infer_unit(name, ""),
            "notes":    "",
        })

    return analyses

def _rebuild_centers_index() -> None:
    """
    Writes data/centers.json — the file the mobile app reads.
    Only includes active centers. Inactive centers are excluded so the
    app never sees placeholder or unsupported entries.
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
    Collected from all written analyses files.
    """
    analyses_dir = DATA_DIR / "analyses"
    if not analyses_dir.exists():
        return

    # Collect all unique analysis names across all centers
    all_names: list[str] = []
    for f in analyses_dir.glob("*.json"):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            all_names.extend(a["name"] for a in data.get("analyses", []))
        except Exception:
            continue

    # Group names that share a common root keyword
    CANONICAL_KEYWORDS: dict[str, list[str]] = {
        "XRD":       ["xrd", "x-ray", "x ışını", "kırınım", "difraksiyon"],
        "SEM":       ["sem", "taramalı elektron", "scanning electron"],
        "SEM-EDX":   ["edx", "eds", "enerji dağılımlı", "energy dispersive"],
        "Kaplama":   ["altın", "karbon", "altın kaplama", "karbon kaplama", "paladyum", "kaplama"],
        "TEM":       ["tem", "transmisyon elektron", "transmission electron"],
        "AFM":       ["afm", "atomik kuvvet", "atomic force"],
        "BET":       ["bet", "yüzey alan", "surface area"],
        "TGA":       ["tga", "termogravimetri", "thermogravimetric", "termal gravimetri"],
        "DSC":       ["dsc", "diferansiyel taramalı kalorimetre", "differential scanning"],
        "DTA":       ["dta", "diferansiyel termal analiz"],
        "FTIR":      ["ftir", "ft-ir", "kızılötesi spektroskopi", "infrared"],
        "Raman":     ["raman"],
        "ICP-MS":    ["icp-ms", "icp ms", "kütle spektrometri"],
        "ICP-OES":   ["icp-oes", "icp oes", "optik emisyon"],
        "XRF":       ["xrf", "x-ışını floresans", "x-ray fluorescence"],
        "Porozimetre":["porozimetre", "porozimetri", "cıvalı", "mercury porosimetry"],
        "Mikrosertlik":["mikrosertlik", "vickers", "hardness"],
        "OES":       ["oes", "optik emisyon spektroskopi"],
        "Eleman Analizi": ["eleman analiz", "elementel", "elemental", "chns"],
        "NMR":       ["nmr", "nükleer manyetik rezonans"],
    }

    # Build the output: for each canonical name, list all matching names from real data
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
