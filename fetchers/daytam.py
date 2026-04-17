"""
Fetcher: DAYTAM — Atatürk Üniversitesi Doğu Anadolu Yüksek Teknoloji Merkezi
https://daytam.atauni.edu.tr/

DAYTAM publishes a bilingual (Turkish + English) PDF price list each year.
pdfplumber produces fragmented tables with inconsistent column counts (4, 8,
15, 18 columns) from this PDF, so standard table extraction fails.

This fetcher:
1. Tries to find the current year's PDF on the hizmet-bedelleri page.
2. Falls back to fallback_pdf_url from the center config.
3. Parses the PDF raw text with a regex-based parser that handles the
   bilingual layout reliably, producing a clean 2-column table that the
   standard json_exporter can process without modification.
"""

import re
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from fetchers.html_then_pdf import _download_pdf, _extract_pdf

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

# Code pattern: 2-12 chars (incl. Turkish), dash, 2 digits, 0-3 asterisks.
# Lookahead (?=\s|...|$) instead of \b so asterisks are captured in the code.
_CODE_RE  = re.compile(r'(?<![^\s(,])([A-ZÇĞİÖŞÜa-z]{2,12}-\d{2}[*]{0,3})(?=\s|[,(]|$)')
_PRICE_RE = re.compile(r'\b(\d{1,3}(?:\.\d{3})*,\d{2})\b')
_UNIT_STRIP = re.compile(
    r'\s*(Adet|Piece|Saat|Hour|Gün|Day|Set|Vial|Flask|Jel|Gel|Tüp|Tube|'
    r'Örnek|Sample|Mikroplaka|Microplate|Membran|Membrane|Günlük|Daily)\s*$',
    re.IGNORECASE,
)

PDF_KEYWORDS = ["daytam", "analiz", "ucret", "fiyat", "liste", "hizmet"]


def _find_pdf_on_page(page_url: str) -> str | None:
    """Try to find the PDF link on the pricing page."""
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
        # Second pass: any .pdf
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf"):
                return href if href.startswith("http") else urljoin(page_url, href)
    except Exception as exc:
        print(f"  [DAYTAM] Sayfa okunamadı ({exc.__class__.__name__})")
    return None


def _pdf_text_to_table(raw_text: str) -> list[list[str]]:
    """
    Parse bilingual DAYTAM PDF raw text into a clean 2-column table:
      [["Analiz Adı", "Ücret (TL)"], [name, price_str], ...]

    The produced price strings ("700,00", "1.400,00") are in Turkish
    format so json_exporter._parse_price handles them directly.
    """
    lines   = [l.strip() for l in raw_text.split("\n")]
    rows    = [["Analiz Adı", "Ücret (TL)"]]
    seen: set[str] = set()

    for i, line in enumerate(lines):
        if not line:
            continue

        # Section headers are all-caps short lines — skip
        if (re.match(r"^[A-ZÇĞİÖŞÜ\s\-–/()]{8,}$", line)
                and not _CODE_RE.search(line)
                and not _PRICE_RE.search(line)
                and len(line.split()) <= 8):
            continue

        code_m = _CODE_RE.search(line)
        if not code_m:
            continue

        # Find price: same line or within the next 3 lines
        price_str = None
        for offset in range(0, 4):
            idx = i + offset
            if idx >= len(lines):
                break
            pm = _PRICE_RE.search(lines[idx])
            if pm:
                price_str = pm.group(1)
                break

        if not price_str:
            continue  # "Daytamla Görüşülecek" — no price, skip

        # Extract name from surrounding text
        after  = _PRICE_RE.sub("", line[code_m.end():]).strip()
        after  = _UNIT_STRIP.sub("", after).strip()
        before = _UNIT_STRIP.sub("", line[:code_m.start()].strip()).strip()

        prev_name = ""
        for j in range(i - 1, max(i - 3, -1), -1):
            prev = lines[j]
            if (prev
                    and not _CODE_RE.search(prev)
                    and not _PRICE_RE.search(prev)
                    and not re.match(r"^[A-Z\s\-–/()*]{10,}$", prev)):
                cleaned = _UNIT_STRIP.sub("", prev).strip()
                if len(cleaned) > 4 and not re.match(r"^[\*\d]", cleaned):
                    prev_name = cleaned
                    break

        name = after if len(after) > 4 else (before if len(before) > 4 else prev_name)

        if not name or len(name) < 4 or re.match(r"^[\*\d]", name):
            continue

        key = f"{name.lower()}_{price_str}"
        if key in seen:
            continue
        seen.add(key)
        rows.append([name, price_str])

    return rows


def fetch(center: dict) -> dict:
    page_url = center.get("pricing_url", center["url"])
    fallback  = center.get("fallback_pdf_url")

    print(f"  [DAYTAM] PDF aranıyor: {page_url}")
    pdf_url = _find_pdf_on_page(page_url)

    if not pdf_url:
        if fallback:
            print(f"  [DAYTAM] Sayfada PDF bulunamadı, yedek URL kullanılıyor.")
            pdf_url = fallback
        else:
            raise ValueError("[DAYTAM] PDF bulunamadı ve yedek URL tanımlanmamış.")

    print(f"  [DAYTAM] PDF indiriliyor: {pdf_url}")
    pdf_bytes = _download_pdf(pdf_url)

    # Always use text-based parsing for DAYTAM's bilingual PDF
    _, raw_text, is_scanned = _extract_pdf(pdf_bytes)

    parsed_table = _pdf_text_to_table(raw_text)
    n = len(parsed_table) - 1  # exclude header
    print(f"  [DAYTAM] Metin ayrıştırma: {n} analiz bulundu.")

    return {
        "center_id":      center["id"],
        "url":            page_url,
        "pdf_url":        pdf_url,
        "tables":         [parsed_table] if n > 0 else [],
        "raw_text":       raw_text,
        "is_scanned_pdf": is_scanned,
    }
