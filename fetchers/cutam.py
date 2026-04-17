"""
Fetcher: CUTAM — Cumhuriyet Üniversitesi İleri Teknoloji Araştırma Merkezi
https://cutam.cumhuriyet.edu.tr/

Scrapes the pricing page for a PDF link; falls back to fallback_pdf_url.
PDFs are stored under userfiles/1/files/ with yearly filenames.
"""

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

PDF_KEYWORDS = ["fiyat", "cutam", "analiz", "ucret", "liste"]


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
        print(f"  [CUTAM] Sayfa okunamadı ({exc.__class__.__name__})")
    return None


def fetch(center: dict) -> dict:
    page_url = center.get("pricing_url", center["url"])
    fallback  = center.get("fallback_pdf_url")

    print(f"  [CUTAM] PDF aranıyor: {page_url}")
    pdf_url = _find_pdf_on_page(page_url)

    if not pdf_url:
        if fallback:
            print(f"  [CUTAM] Sayfada PDF bulunamadı, yedek URL kullanılıyor.")
            pdf_url = fallback
        else:
            raise ValueError("[CUTAM] PDF bulunamadı ve yedek URL tanımlanmamış.")

    print(f"  [CUTAM] PDF indiriliyor: {pdf_url}")
    pdf_bytes = _download_pdf(pdf_url)
    tables, raw_text, is_scanned = _extract_pdf(pdf_bytes)

    return {
        "center_id":      center["id"],
        "url":            page_url,
        "pdf_url":        pdf_url,
        "tables":         tables,
        "raw_text":       raw_text,
        "is_scanned_pdf": is_scanned,
    }
