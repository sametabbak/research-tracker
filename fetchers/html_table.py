"""
Fetcher: html_table
Scrapes price tables directly from an HTML page.
Used by: DPÜ İLTEM, ESOGÜ ARUM

Supports `fallback_selectors` in center config: when the primary selector
finds no tables, each fallback selector is tried in order.
Useful for pages using TablePress, Gutenberg blocks, or other non-standard
table markup.
"""

import time
import requests
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
    "Cache-Control":   "no-cache",
}

MAX_RETRIES   = 3
RETRY_BACKOFF = 2
TIMEOUT       = 25

ERROR_PHRASES = [
    "bakım", "maintenance", "erişilemiyor", "bulunamadı",
    "forbidden", "503", "geçici olarak",
]


# ── Public interface ──────────────────────────────────────────────────────────

def fetch(center: dict) -> dict:
    """
    Fetch and extract all price tables from the pricing page.
    Tries the primary selector first, then each fallback selector in order.

    Returns:
    {
        "center_id": str,
        "url":       str,
        "tables":    [ [ [row], ... ], ... ],
        "raw_text":  str
    }
    """
    url               = center["pricing_url"]
    primary_selector  = center.get("selector", "table")
    fallback_selectors = center.get("fallback_selectors", [])

    html = _get_with_retry(url)
    _check_for_error_page(html, url)

    soup = BeautifulSoup(html, "html.parser")

    # Try primary selector
    tables = _extract_tables(soup, primary_selector)

    if not tables:
        print(f"  [{center['id']}] '{primary_selector}' ile tablo bulunamadı.")

        # Try each fallback selector in order
        for fallback in fallback_selectors:
            print(f"  [{center['id']}] Yedek deneniyor: '{fallback}'")
            tables = _extract_tables(soup, fallback)
            if tables:
                print(f"  [{center['id']}] ✅ '{fallback}' ile {len(tables)} tablo bulundu.")
                break

    raw_text = _extract_text(soup)

    return {
        "center_id": center["id"],
        "url":       url,
        "tables":    tables,
        "raw_text":  raw_text,
    }


# ── HTTP helpers ──────────────────────────────────────────────────────────────

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


# ── Parsing helpers ───────────────────────────────────────────────────────────

def _extract_tables(soup: BeautifulSoup, selector: str) -> list[list[list[str]]]:
    tables = []
    for table in soup.select(selector):
        rows = []
        for tr in table.find_all("tr"):
            cells = [
                td.get_text(separator=" ", strip=True)
                for td in tr.find_all(["td", "th"])
            ]
            if not any(cells):
                continue
            # Skip merged header rows (single cell spanning all columns)
            if len(cells) == 1 and tr.find(attrs={"colspan": True}):
                continue
            rows.append(cells)
        if len(rows) > 1:
            tables.append(rows)
    return tables


def _extract_text(soup: BeautifulSoup) -> str:
    for tag in soup(["nav", "header", "footer", "script", "style", "aside"]):
        tag.decompose()
    return soup.get_text(separator="\n", strip=True)
