"""
Fetcher: html_then_pdf
Loads the pricing page, extracts the current PDF link dynamically,
downloads and parses the PDF.
Used by: AKÜ TUAM, ESTÜ SAM

Improvements over v1:
  - Retry with backoff on both the HTML page and the PDF download
  - Detects scanned (image-only) PDFs and warns clearly
  - Falls back to raw text extraction if table extraction fails
  - Resolves relative URLs correctly
  - Verifies the PDF link before downloading
"""

import io
import time
from urllib.parse import urljoin, urlparse
import requests
import pdfplumber
from bs4 import BeautifulSoup

# ── Constants ────────────────────────────────────────────────────────────────

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
TIMEOUT_PDF   = 45   # PDFs can be large

# Minimum characters per page — below this threshold the PDF is likely scanned
SCANNED_PDF_CHARS_THRESHOLD = 50


# ── Public interface ─────────────────────────────────────────────────────────

def fetch(center: dict) -> dict:
    """
    1. Fetch HTML pricing page
    2. Find PDF link matching pdf_link_pattern
    3. Download and extract PDF content

    Returns:
    {
        "center_id": str,
        "url":       str,    # HTML page URL
        "pdf_url":   str,    # resolved PDF URL
        "tables":    [...],
        "raw_text":  str,
        "is_scanned_pdf": bool   # True if OCR would be needed for full data
    }
    """
    page_url = center["pricing_url"]
    pattern  = center.get("pdf_link_pattern", "")

    # Step 1 — fetch HTML page
    html = _get_with_retry(page_url, TIMEOUT_HTML)
    soup = BeautifulSoup(html, "html.parser")

    # Step 2 — find PDF link
    pdf_url = _find_pdf_link(soup, page_url, pattern)
    if not pdf_url:
        raise ValueError(
            f"[{center['id']}] No PDF link matching '{pattern}' found on {page_url}\n"
            f"  Links found: {[a['href'] for a in soup.find_all('a', href=True)][:10]}"
        )

    # Step 3 — download PDF
    pdf_bytes = _download_pdf(pdf_url)

    # Step 4 — extract content
    tables, raw_text, is_scanned = _extract_pdf(pdf_bytes)

    if is_scanned:
        print(
            f"  ⚠️  [{center['id']}] PDF appears to be scanned (image-based). "
            "Text extraction is limited. Consider adding OCR support."
        )

    return {
        "center_id":      center["id"],
        "url":            page_url,
        "pdf_url":        pdf_url,
        "tables":         tables,
        "raw_text":       raw_text,
        "is_scanned_pdf": is_scanned,
    }


# ── HTTP helpers ─────────────────────────────────────────────────────────────

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
                    "The PDF link may have changed."
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
    """
    Find first <a href> that:
      - Contains the pattern (case-insensitive)
      - Ends with .pdf
      - Is a valid URL (absolute or relative)
    """
    for a in soup.find_all("a", href=True):
        href: str = a["href"].strip()
        if not href:
            continue
        if pattern.lower() in href.lower() and href.lower().endswith(".pdf"):
            if href.startswith("http"):
                return href
            return urljoin(base_url, href)

    # Second pass — look for any .pdf link if pattern not found
    print(f"  Pattern '{pattern}' not matched, trying any .pdf link...")
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().endswith(".pdf"):
            return href if href.startswith("http") else urljoin(base_url, href)

    return None


# ── PDF content extraction ────────────────────────────────────────────────────

def _extract_pdf(pdf_bytes: bytes) -> tuple[list, str, bool]:
    """
    Extract tables and text from a PDF.
    Returns (tables, raw_text, is_scanned).
    """
    tables: list       = []
    text_parts: list   = []
    total_chars        = 0
    total_pages        = 0

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        total_pages = len(pdf.pages)

        for page in pdf.pages:
            # ── Table extraction ──────────────────────────────────────────
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

            # ── Text extraction ───────────────────────────────────────────
            text = page.extract_text(x_tolerance=3, y_tolerance=3)
            if text:
                text_parts.append(text)
                total_chars += len(text.strip())

    raw_text = "\n\n".join(text_parts)

    # Detect scanned PDF: fewer than threshold chars per page on average
    avg_chars = total_chars / max(total_pages, 1)
    is_scanned = avg_chars < SCANNED_PDF_CHARS_THRESHOLD

    # If scanned, tables will be empty but raw_text might still have partial OCR
    return tables, raw_text, is_scanned
