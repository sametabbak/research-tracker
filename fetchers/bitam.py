"""
Fetcher: BİTAM — Necmettin Erbakan Üniversitesi Bilim ve Teknoloji Merkezi
https://erbakan.edu.tr/tr/birim/bitam

Scrapes the pricing page for the annual PDF link, then parses it.
Each pdfplumber table has 3 cols: Analiz Kodu | Analiz Tanımı | Fiyat.
The fetcher reads page text to find the section header immediately preceding
each table and injects it as a Category column so json_exporter can use it.
"""

import io
import re
from urllib.parse import urljoin

import pdfplumber
import requests
from bs4 import BeautifulSoup

from fetchers.html_then_pdf import _download_pdf

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

PDF_KEYWORDS = ["bitam", "analiz", "ucret", "fiyat", "liste"]

# Known document-level header phrases to exclude from section detection
_EXCLUDE_PHRASES = [
    "NECMETTİN ERBAKAN", "ÜNİVERSİTESİ BİLİM", "ARAŞTIRMA MERKEZİ",
    "UYGULAMA MERKEZİ", "KONYA", "2026 YILI", "ÖNEMLİ HUSUSLAR",
]

# A genuine section header:
# - All caps (Turkish chars OK)
# - Not a table header row (doesn't contain "KODU" and "TANIMI" together)
# - Not a page number
# - Not a known document header
def _is_section_header(line: str) -> bool:
    line = line.strip()
    if not line or len(line) < 6:
        return False
    if re.match(r'^Sayfa\s+\d', line, re.IGNORECASE):
        return False
    if not re.match(r'^[A-ZÇĞİÖŞÜ0-9\s\-–/()*\.]+$', line):
        return False
    lower = line.lower()
    if "kodu" in lower and "tanımı" in lower:  # table header row
        return False
    if any(exc in line for exc in _EXCLUDE_PHRASES):
        return False
    return True


def _find_pdf_on_page(page_url: str) -> str | None:
    try:
        r = requests.get(page_url, headers=HEADERS, timeout=20, allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href: str = a["href"]
            if not href.lower().endswith(".pdf"):
                continue
            combined = (href + " " + a.get_text()).lower()
            if any(k in combined for k in PDF_KEYWORDS):
                return href if href.startswith("http") else urljoin(page_url, href)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf"):
                return href if href.startswith("http") else urljoin(page_url, href)
    except Exception as exc:
        print(f"  [BİTAM] Sayfa okunamadı ({exc.__class__.__name__})")
    return None


def _extract_with_categories(pdf_bytes: bytes) -> list[list[list[str]]]:
    """
    Parse BİTAM PDF. Returns tables with header:
      ["Kategori", "Analiz Tanımı", "Fiyat"]

    Use with column_name_map:
      {"category": "kategori", "name": "analiz tanımı", "price": "fiyat"}
    """
    result_tables: list[list[list[str]]] = []

    # Build a global list of (section_header, [analysis_rows]) by scanning
    # all pages' raw text. We use raw text (not pdfplumber tables) to track
    # section headers, then map them onto the pdfplumber table extraction.

    # Pass 1: build text-based section → analysis mapping
    # to know which section header precedes each data row
    section_map: dict[str, str] = {}  # analysis_name_lower → category
    current_section = ""

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            raw = page.extract_text(x_tolerance=3, y_tolerance=3) or ""
            lines = [l.strip() for l in raw.split("\n") if l.strip()]
            for line in lines:
                if _is_section_header(line):
                    current_section = line.rstrip("*").strip()
                else:
                    # If looks like an analysis row (has a price), map it
                    if "₺" in line or "TL" in line:
                        # Extract name portion (before the price)
                        name_part = re.sub(r'\s+\d[\d\.,]*\s*₺.*$', '', line).strip()
                        if len(name_part) > 4:
                            section_map[name_part.lower()[:40]] = current_section

    # Pass 2: extract pdfplumber tables and assign categories
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            for raw_table in page.extract_tables():
                if not raw_table or len(raw_table) < 2:
                    continue

                # Clean rows
                cleaned: list[list[str]] = []
                for row in raw_table:
                    clean = [(c.strip().replace("\n", " ") if c else "") for c in row]
                    if any(clean):
                        cleaned.append(clean)

                if len(cleaned) < 2 or len(cleaned[0]) != 3:
                    continue

                # Skip if header row doesn't look like Kod|Tanım|Fiyat
                h = cleaned[0]
                if not any("analiz" in (v or "").lower() for v in h):
                    continue

                out_table: list[list[str]] = [["Kategori", "Analiz Tanımı", "Fiyat"]]

                for row in cleaned[1:]:
                    if len(row) < 3:
                        continue
                    name  = row[1].strip()
                    price = row[2].strip()
                    if not name or not price:
                        continue

                    # Look up category from pass-1 map
                    cat = section_map.get(name.lower()[:40], "")
                    if not cat:
                        # Fuzzy fallback: try first 15 chars
                        key15 = name.lower()[:15]
                        for k, v in section_map.items():
                            if key15 in k:
                                cat = v
                                break

                    out_table.append([cat, name, price])

                if len(out_table) > 1:
                    result_tables.append(out_table)

    return result_tables


def fetch(center: dict) -> dict:
    page_url = center.get("pricing_url", center["url"])
    fallback  = center.get("fallback_pdf_url")

    print(f"  [BİTAM] PDF aranıyor: {page_url}")
    pdf_url = _find_pdf_on_page(page_url)

    if not pdf_url:
        if fallback:
            print(f"  [BİTAM] Sayfada PDF bulunamadı, yedek URL kullanılıyor.")
            pdf_url = fallback
        else:
            raise ValueError("[BİTAM] PDF bulunamadı ve yedek URL tanımlanmamış.")

    print(f"  [BİTAM] PDF indiriliyor: {pdf_url}")
    pdf_bytes = _download_pdf(pdf_url)
    tables    = _extract_with_categories(pdf_bytes)

    # Raw text for diagnostics
    raw_text_parts = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text(x_tolerance=3, y_tolerance=3)
            if t:
                raw_text_parts.append(t)

    total = sum(len(t) - 1 for t in tables)
    print(f"  [BİTAM] {len(tables)} tablo, {total} analiz ayıklandı.")

    return {
        "center_id": center["id"],
        "url":       page_url,
        "pdf_url":   pdf_url,
        "tables":    tables,
        "raw_text":  "\n\n".join(raw_text_parts),
    }
