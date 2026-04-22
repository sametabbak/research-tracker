"""
Fetcher: GAZI_GUTMAM
Açıklama: Bu dosya add_center.py sihirbazı tarafından otomatik oluşturuldu.
Hedef URL: https://tmbil.gazi.edu.tr/view/page/287476/analiz-listesi-ucretlendirme
"""

import time
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
}

MAX_RETRIES = 3
RETRY_BACKOFF = 2
TIMEOUT = 25

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
    raise last_exc

def fetch(center: dict) -> dict:
    url = center.get("pricing_url") or center["url"]
    
    # 1. Sayfayı çek
    html = _get_with_retry(url)
    soup = BeautifulSoup(html, "html.parser")
    
    tables = []
    # 2. TABLO KAZIMA MANTIĞINI BURAYA YAZ (örn. soup.select("table"))
    # Örnek: tables = [[["Test Hizmeti", "100 TL", "Adet"]]]
    
    raw_text = soup.get_text(separator="\n", strip=True)

    return {
        "center_id": center["id"],
        "url":       url,
        "tables":    tables,
        "raw_text":  raw_text,
    }
