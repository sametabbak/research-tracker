"""
Fetcher: TAUM — Erciyes Üniversitesi Teknoloji Araştırma ve Uygulama Merkezi
https://taum.erciyes.edu.tr/

PDF URL follows a clean predictable pattern:
  https://taum.erciyes.edu.tr/Dosya/MainContent/fiyat-listesi-{year}.pdf
No page scraping needed; tries current year then falls back one year.
"""

from datetime import datetime
from urllib.parse import urljoin

from fetchers.html_then_pdf import _download_pdf, _extract_pdf


def fetch(center: dict) -> dict:
    year = datetime.now().year

    pdf_url    = None
    pdf_bytes  = None

    for try_year in [year, year - 1]:
        url = (
            f"https://taum.erciyes.edu.tr/Dosya/MainContent/"
            f"fiyat-listesi-{try_year}.pdf"
        )
        try:
            pdf_bytes = _download_pdf(url)
            pdf_url   = url
            print(f"  [TAUM] PDF bulundu ({try_year}): {url}")
            break
        except Exception as exc:
            print(f"  [TAUM] {url} — {exc.__class__.__name__}, sonraki yıl deneniyor...")

    if not pdf_bytes or not pdf_url:
        fallback = center.get("fallback_pdf_url")
        if fallback:
            print(f"  [TAUM] Yedek URL kullanılıyor: {fallback}")
            pdf_bytes = _download_pdf(fallback)
            pdf_url   = fallback
        else:
            raise ValueError(
                f"[TAUM] {year} ve {year - 1} için PDF bulunamadı ve "
                "yedek URL tanımlanmamış."
            )

    tables, raw_text, is_scanned = _extract_pdf(pdf_bytes)

    return {
        "center_id":      center["id"],
        "url":            center.get("pricing_url", center.get("url")),
        "pdf_url":        pdf_url,
        "tables":         tables,
        "raw_text":       raw_text,
        "is_scanned_pdf": is_scanned,
    }
