"""
Fetcher: İKCU MERLAB — İzmir Katip Çelebi Üniversitesi Merkezi Araştırma Lab.
https://merkeziarastirmalab.ikcu.edu.tr/

Pricing page: https://merkeziarastirmalab.ikcu.edu.tr/S/15286/ucretlendirme
Uses an HTML table on the pricing page — no PDF provided.
Tries multiple selectors; the page is a CMS-based university site.
"""

import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
}

MAX_RETRIES   = 3
RETRY_BACKOFF = 2
TIMEOUT       = 25

SELECTORS = [
    "table",
    ".tablepress",
    ".wp-block-table table",
    ".entry-content table",
    ".content table",
    "article table",
    "#content table",
    "main table",
]


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
    raise last_exc  # type: ignore


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
            if len(cells) == 1 and tr.find(attrs={"colspan": True}):
                continue
            rows.append(cells)
        if len(rows) > 1:
            tables.append(rows)
    return tables


def fetch(center: dict) -> dict:
    url = center.get("pricing_url", center["url"])

    html = _get_with_retry(url)
    soup = BeautifulSoup(html, "html.parser")

    tables = []
    used_selector = None
    for selector in SELECTORS:
        tables = _extract_tables(soup, selector)
        if tables:
            used_selector = selector
            print(f"  [İKCU MERLAB] '{selector}' ile {len(tables)} tablo bulundu.")
            break

    if not tables:
        print(
            f"  [İKCU MERLAB] ⚠️ Hiçbir seçici tablo bulamadı. "
            "Sayfa JavaScript ile render ediliyor olabilir."
        )

    raw_text = soup.get_text(separator="\n", strip=True)

    return {
        "center_id": center["id"],
        "url":       url,
        "tables":    tables,
        "raw_text":  raw_text,
    }
