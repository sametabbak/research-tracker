"""
Fetcher: GAZI_GUTMAM
Açıklama: Gazi Üniversitesi GÜTMAM Fiyat Listesi Çekici
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
            # Üniversite sitelerinde bazen SSL sertifika sorunları yaşanabilir.
            # verify=False ekleyerek bu sorunları es geçebiliriz. (Gerekirse açılır)
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
    
    html = _get_with_retry(url)
    soup = BeautifulSoup(html, "html.parser")
    
    tables = []
    
    # GÜTMAM sayfasındaki tüm tabloları sırayla yakala
    for table_tag in soup.find_all("table"):
        rows = []
        for tr in table_tag.find_all("tr"):
            # Her satırdaki hücreleri (th veya td) al, içindeki metni temizle
            cells = [
                td.get_text(separator=" ", strip=True) 
                for td in tr.find_all(["td", "th"])
            ]
            
            # Tamamen boş satırları atla
            if not any(cells):
                continue
                
            # GÜTMAM sitesinde "GÖRÜNTÜLEME LABORATUVARI" gibi tüm satırı kaplayan 
            # tek hücreli üst başlıklar var. Bunları analize dahil etmeyip atlıyoruz.
            if len(cells) == 1 and tr.find(attrs={"colspan": True}):
                continue
                
            rows.append(cells)
            
        # Sadece içi dolu tabloları (en az 2 satır veri içeren) listeye ekle
        if len(rows) > 1:
            tables.append(rows)

    raw_text = soup.get_text(separator="\n", strip=True)

    # Botun ekrana kaç tablo bulduğunu yazdırmasını sağlayalım (hata ayıklama için harika bir yöntemdir)
    print(f"  [GÜTMAM] Sayfada {len(tables)} adet tablo başarıyla ayıklandı.")

    return {
        "center_id": center["id"],
        "url":       url,
        "tables":    tables,
        "raw_text":  raw_text,
    }
