"""
Fetcher: html_table
Scrapes price tables directly from an HTML page.
Used by: DPÜ İLTEM, ESOGÜ ARUM

Improvements over v1:
  - Session-based requests (handles cookies automatically)
  - Retry with exponential backoff
  - Encoding detection via chardet / apparent_encoding
  - Detects maintenance/error pages before parsing
  - Strips merged header rows that span all columns
"""

import time
import requests
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
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control":   "no-cache",
}

MAX_RETRIES   = 3
RETRY_BACKOFF = 2   # seconds; doubles each attempt
TIMEOUT       = 25  # seconds per request

# Phrases that indicate we got a maintenance/error page instead of real content
ERROR_PHRASES = [
    "bakım", "maintenance", "erişilemiyor", "bulunamadı", "404",
    "forbidden", "503", "geçici olarak",
]


# ── Public interface ─────────────────────────────────────────────────────────

def fetch(center: dict) -> dict:
    """
    Fetch and extract all price tables from the pricing page.

    Returns:
    {
        "center_id": str,
        "url":       str,
        "tables":    [ [ [cell,...], ... ], ... ],
        "raw_text":  str
    }

    Raises requests.HTTPError or ValueError on unrecoverable failure.
    """
    url      = center["pricing_url"]
    selector = center.get("selector", "table")

    html = _get_with_retry(url)
    _check_for_error_page(html, url)

    soup = BeautifulSoup(html, "html.parser")
    tables   = _extract_tables(soup, selector)
    raw_text = _extract_text(soup)

    return {
        "center_id": center["id"],
        "url":       url,
        "tables":    tables,
        "raw_text":  raw_text,
    }


# ── HTTP helpers ─────────────────────────────────────────────────────────────

def _get_with_retry(url: str) -> str:
    session = requests.Session()
    session.headers.update(HEADERS)

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT, allow_redirects=True)
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


def _check_for_error_page(html: str, url: str) -> None:
    snippet = html[:2000].lower()
    for phrase in ERROR_PHRASES:
        if phrase in snippet and "<table" not in html[:5000].lower():
            raise ValueError(
                f"Possible error/maintenance page at {url} (found '{phrase}' near top)"
            )


# ── Parsing helpers ──────────────────────────────────────────────────────────

def _extract_tables(soup: BeautifulSoup, selector: str) -> list[list[list[str]]]:
    tables = []
    for table in soup.select(selector):
        rows = []
        for tr in table.find_all("tr"):
            cells = [
                td.get_text(separator=" ", strip=True)
                for td in tr.find_all(["td", "th"])
            ]
            # Skip completely empty rows
            if not any(cells):
                continue
            # Skip merged header rows (single cell spanning all columns via colspan)
            if len(cells) == 1 and tr.find(attrs={"colspan": True}):
                continue
            rows.append(cells)
        if len(rows) > 1:   # need at least a header + one data row
            tables.append(rows)
    return tables


def _extract_text(soup: BeautifulSoup) -> str:
    for tag in soup(["nav", "header", "footer", "script", "style", "aside"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)
