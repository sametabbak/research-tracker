"""
Fetcher: HACETTEPE_HUNITEK
Açıklama: Hacettepe Üniversitesi HÜNİTEK PDF Fiyat Listesi Çekici (Kategori ve Tanım Ayrımı)
"""

import io
import time
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

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

def _get_with_retry(url: str, is_pdf=False):
    session = requests.Session()
    session.headers.update(HEADERS)
    
    # Üniversite sitelerindeki SSL uyarılarını kapatıyoruz
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT, allow_redirects=True, verify=False)
            resp.raise_for_status()
            if is_pdf:
                return resp.content
            resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except requests.RequestException as exc:
            last_exc = exc
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF ** attempt)
    raise last_exc

def fetch(center: dict) -> dict:
    if pdfplumber is None:
        raise ImportError("⚠️ HATA: 'pip install pdfplumber' kurmanız gerekli.")

    main_url = center.get("url", "https://hunitek.hacettepe.edu.tr/")
    pdf_url = "https://hunitek.hacettepe.edu.tr/wp-content/uploads/2026/01/2026_hunitek_hizmet-bedelleri.pdf"
    
    try:
        html = _get_with_retry(main_url)
        soup = BeautifulSoup(html, "html.parser")
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if ".pdf" in href.lower() and "hizmet-bedelleri" in href.lower():
                pdf_url = href if href.startswith("http") else urljoin(main_url, href)
                break
    except:
        pass

    pdf_content = _get_with_retry(pdf_url, is_pdf=True)
    
    # json_exporter'ın kusursuz tanıması için başlıkları standart isimlerle belirliyoruz.
    perfect_rows = [["Kategori", "İşlem Türü", "Ücret"]] 
    raw_text = ""
    
    with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
        last_category = "Genel Analizler" # Boş gelen ilk hücreler için varsayılan kategori hafızası
        
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                raw_text += text + "\n"
            
            extracted_tables = page.extract_tables()
            for table in extracted_tables:
                for row in table:
                    clean_row = [str(cell).strip().replace("\n", " ") if cell else "" for cell in row]
                    
                    if not any(clean_row):
                        continue
                    
                    # 1. Fiyat hücresini bul ("TL" ve rakam içeriyor mu?)
                    price_col_idx = -1
                    for i, cell in enumerate(clean_row):
                        if "TL" in cell.upper() and any(c.isdigit() for c in cell):
                            price_col_idx = i
                            break
                    
                    # Fiyat hücresini bulduysak işlemi başlat
                    if price_col_idx > 0:
                        # Durum A: Fiyat sütunundan önce en az 2 sütun var (0: Deney/Cihaz, 1... : Tanım)
                        if price_col_idx >= 2:
                            cat_val = clean_row[0].strip()
                            
                            # Eğer "Deney/Metot" hücresi doluysa (ve tablo başlığı değilse) hafızayı güncelle
                            if cat_val and len(cat_val) > 2 and "DENEY" not in cat_val.upper():
                                last_category = cat_val
                            
                            # Kalan aradaki sütunları birleştirip "Tanım" (İşlem Türü) yap
                            name_val = " ".join(clean_row[1:price_col_idx]).strip()
                            price_val = clean_row[price_col_idx]
                            
                            # Eğer kazara başlık satırını çekmediysek listeye ekle
                            if len(name_val) > 2 and "TANIM" not in name_val.upper():
                                perfect_rows.append([last_category, name_val, price_val])
                                
                        # Durum B: Fiyat sütunundan önce sadece 1 sütun var (Yani sadece Tanım var, kategori hücresi yutulmuş)
                        elif price_col_idx == 1:
                            name_val = clean_row[0].strip()
                            price_val = clean_row[price_col_idx]
                            
                            if len(name_val) > 2 and "TANIM" not in name_val.upper():
                                # Hafızadaki son cihazı (last_category) kategori olarak ata
                                perfect_rows.append([last_category, name_val, price_val])
                                
                    # Yedek Plan: Sadece rakam olan ama TL yazmayan fiyatları yakala
                    elif len(clean_row) >= 2:
                        price_val = clean_row[-1]
                        if any(c.isdigit() for c in price_val):
                            if len(clean_row) >= 3:
                                cat_val = clean_row[0].strip()
                                if cat_val and len(cat_val) > 2 and "DENEY" not in cat_val.upper():
                                    last_category = cat_val
                                name_val = " ".join(clean_row[1:-1]).strip()
                            else:
                                name_val = clean_row[0].strip()
                                
                            if len(name_val) > 2 and "TANIM" not in name_val.upper():
                                perfect_rows.append([last_category, name_val, price_val])

    tables = [perfect_rows]

    print(f"  [HÜNİTEK] Kategori ve Tanım ayrımlı akıllı okuma tamamlandı. {len(perfect_rows)-1} adet analiz çıkarıldı.")

    return {
        "center_id": center["id"],
        "url":       pdf_url,
        "tables":    tables,
        "raw_text":  raw_text,
    }
