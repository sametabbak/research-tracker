"""
Fetcher: html_then_pdf
Loads the pricing page, extracts the current PDF link dynamically,
downloads and parses the PDF.
Used by: AKÜ TUAM, ESTÜ SAM

If the HTML page is unreachable (e.g. GitHub Actions can't reach .edu.tr),
falls back to `fallback_pdf_url` in the center config.
Update `fallback_pdf_url` manually each year when the PDF URL changes.
"""

import io
import time
from urllib.parse import urljoin
import requests
import pdfplumber
from bs4 import BeautifulSoup

# ── Constants ─────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
}

PDF_HEADERS = {
    **HEADERS,
    "Accept": "application/pdf,application/octet-stream,*/*",
}

MAX_RETRIES   = 3
RETRY_BACKOFF = 2
TIMEOUT_HTML  = 25
TIMEOUT_PDF   = 45

SCANNED_PDF_CHARS_THRESHOLD = 50


# ── Public interface ──────────────────────────────────────────────────────────

def fetch(center: dict) -> dict:
    """
    1. Try to fetch the HTML pricing page and find the PDF link dynamically.
    2. If the HTML page is unreachable, fall back to `fallback_pdf_url` in config.
    3. Download and extract the PDF.
    """
    page_url         = center["pricing_url"]
    pattern          = center.get("pdf_link_pattern", "")
    fallback_pdf_url = center.get("fallback_pdf_url")

    pdf_url = None

    # Step 1 — try to load the HTML page
    try:
        html = _get_with_retry(page_url, TIMEOUT_HTML)
        soup = BeautifulSoup(html, "html.parser")
        pdf_url = _find_pdf_link(soup, page_url, pattern)

        if pdf_url:
            print(f"  [{center['id']}] PDF linki bulundu: {pdf_url}")
        else:
            print(f"  [{center['id']}] Sayfada PDF linki bulunamadı.")

    except Exception as e:
        if fallback_pdf_url:
            print(
                f"  ⚠️  [{center['id']}] HTML sayfası erişilemez ({e.__class__.__name__}). "
                f"Yedek PDF URL kullanılıyor: {fallback_pdf_url}"
            )
            pdf_url = fallback_pdf_url
        else:
            raise

    # Step 2 — if HTML loaded but no PDF link found, try fallback
    if not pdf_url:
        if fallback_pdf_url:
            print(
                f"  ⚠️  [{center['id']}] PDF linki bulunamadı. "
                f"Yedek URL kullanılıyor: {fallback_pdf_url}"
            )
            pdf_url = fallback_pdf_url
        else:
            raise ValueError(
                f"[{center['id']}] PDF linki bulunamadı ve yedek URL tanımlanmamış.\n"
                f"  config/centers.json dosyasına 'fallback_pdf_url' ekleyin."
            )

    # Step 3 — download PDF
    pdf_bytes = _download_pdf(pdf_url)

    # Step 4 — extract content
    tables, raw_text, is_scanned = _extract_pdf(pdf_bytes)

    if is_scanned:
        print(
            f"  ⚠️  [{center['id']}] PDF taranmış görünüyor (görüntü tabanlı). "
            "Metin çıkarımı sınırlı olabilir."
        )

    return {
        "center_id":      center["id"],
        "url":            page_url,
        "pdf_url":        pdf_url,
        "tables":         tables,
        "raw_text":       raw_text,
        "is_scanned_pdf": is_scanned,
    }


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _get_with_retry(url: str, timeout: int) -> str:
    session = requests.Session()
    session.headers.update(HEADERS)
    last_exc = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=timeout, allow_redirects=True)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF ** attempt
                print(f"  Attempt {attempt} failed ({exc}), retrying in {wait}s...")
                time.sleep(wait)

    raise last_exc  # type: ignore[misc]


def _download_pdf(url: str) -> bytes:
    session = requests.Session()
    session.headers.update(PDF_HEADERS)
    last_exc = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT_PDF, allow_redirects=True)
            resp.raise_for_status()

            content_type = resp.headers.get("Content-Type", "")
            if "html" in content_type:
                raise ValueError(
                    f"Expected PDF but got HTML from {url}. "
                    "The PDF link may have changed — update fallback_pdf_url in centers.json."
                )
            return resp.content
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF ** attempt
                print(f"  PDF download attempt {attempt} failed, retrying in {wait}s...")
                time.sleep(wait)

    raise last_exc  # type: ignore[misc]


# ── PDF link extraction ───────────────────────────────────────────────────────

def _find_pdf_link(soup: BeautifulSoup, base_url: str, pattern: str) -> str | None:
    """Find first <a href> matching the pattern that ends with .pdf."""
    for a in soup.find_all("a", href=True):
        href: str = a["href"].strip()
        if not href:
            continue
        if pattern.lower() in href.lower() and href.lower().endswith(".pdf"):
            return href if href.startswith("http") else urljoin(base_url, href)

    # Second pass — any .pdf link
    print(f"  Pattern '{pattern}' not matched, trying any .pdf link...")
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".pdf"):
            return href if href.startswith("http") else urljoin(base_url, href)

    return None


# ── PDF content extraction ────────────────────────────────────────────────────

def _extract_pdf(pdf_bytes: bytes) -> tuple[list, str, bool]:
    tables: list     = []
    text_parts: list = []
    total_chars      = 0
    total_pages      = 0

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        total_pages = len(pdf.pages)

        for page in pdf.pages:
            for raw_table in page.extract_tables():
                if not raw_table:
                    continue
                cleaned = []
                for row in raw_table:
                    clean_row = [
                        (cell.strip().replace("\n", " ") if cell else "")
                        for cell in row
                    ]
                    if any(clean_row):
                        cleaned.append(clean_row)
                if len(cleaned) > 1:
                    tables.append(cleaned)

            text = page.extract_text(x_tolerance=3, y_tolerance=3)
            if text:
                text_parts.append(text)
                total_chars += len(text.strip())

    raw_text   = "\n\n".join(text_parts)
    avg_chars  = total_chars / max(total_pages, 1)
    is_scanned = avg_chars < SCANNED_PDF_CHARS_THRESHOLD

    return tables, raw_text, is_scanned
