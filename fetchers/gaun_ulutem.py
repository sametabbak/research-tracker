"""
Fetcher: GAUN_ULUTEM
Açıklama: Gaziantep Üniversitesi ULUTEM Fiyat Listesi Çekici
Özellik: Rowspan çözümleyici ve Laboratuvar->Kategori eşleştirmesi.
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
    
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT, allow_redirects=True, verify=False)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF ** attempt
                time.sleep(wait)
    raise last_exc

def parse_html_table_to_grid(table_tag):
    """
    HTML tablosundaki rowspan (satır birleştirme) özelliklerini çözerek 
    boşlukları doldurur ve kusursuz bir 2D liste döndürür.
    """
    rows = table_tag.find_all('tr')
    grid = []
    
    for r_idx, row in enumerate(rows):
        cells = row.find_all(['th', 'td'])
        c_idx = 0
        for cell in cells:
            while len(grid) > r_idx and len(grid[r_idx]) > c_idx and grid[r_idx][c_idx] is not None:
                c_idx += 1
                
            rowspan = int(cell.get('rowspan', 1))
            colspan = int(cell.get('colspan', 1))
            text = cell.get_text(separator=' ', strip=True)
            
            for i in range(rowspan):
                for j in range(colspan):
                    while len(grid) <= r_idx + i:
                        grid.append([])
                    while len(grid[r_idx + i]) <= c_idx + j:
                        grid[r_idx + i].append(None)
                    grid[r_idx + i][c_idx + j] = text
            
            c_idx += colspan
            
    return grid

def fetch(center: dict) -> dict:
    url = center.get("pricing_url") or center["url"]
    
    html = _get_with_retry(url)
    soup = BeautifulSoup(html, "html.parser")
    
    perfect_rows = [["Kategori", "Analiz Adı", "Fiyat"]]
    
    tables = soup.find_all("table")
    main_table = None
    for tbl in tables:
        # Doğru tabloyu bulmak için belirleyici başlıkları arıyoruz
        if "Laboratuvar Adı" in tbl.text and "Analiz Adı" in tbl.text:
            main_table = tbl
            break
            
    if not main_table and tables:
        main_table = tables[0]
        
    if main_table:
        grid = parse_html_table_to_grid(main_table)
        
        for row in grid:
            # ULUTEM Sütunları:
            # 0: Laboratuvar Adı, 1: Cihaz Adı, 2: Analiz Adı, 3: Kod, 4: Hizmet Bedeli, 5: Form
            if len(row) >= 5:
                kategori = str(row[0] or "").strip()
                analiz_adi = str(row[2] or "").strip()
                fiyat = str(row[4] or "").strip()
                
                # Tablo başlıklarını veya analiz adı boş olan satırları atla
                if "Laboratuvar Adı" in kategori or not analiz_adi:
                    continue
                    
                # Eğer fiyat sütununda rakam yoksa atla
                if not any(char.isdigit() for char in fiyat):
                    continue
                
                # Kategorisi tamamen boş gelen (hatalı HTML) satırlar için varsayılan bir isim
                if not kategori:
                    kategori = "Genel Analizler"
                
                perfect_rows.append([kategori, analiz_adi, fiyat])
                
    raw_text = soup.get_text(separator="\n", strip=True)

    print(f"  [ULUTEM] Matris çözüldü: Laboratuvarlar kategori yapıldı. {len(perfect_rows)-1} adet analiz çıkarıldı.")

    return {
        "center_id": center["id"],
        "url":       url,
        "tables":    [perfect_rows],
        "raw_text":  raw_text,
    }