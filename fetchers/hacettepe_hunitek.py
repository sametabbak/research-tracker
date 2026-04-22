"""
Fetcher: HACETTEPE_HUNITEK
Açıklama: Hacettepe Üniversitesi HÜNİTEK PDF Fiyat Listesi Çekici (Gelişmiş Tablo Okuma)
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
    
    # json_exporter'ın tablo başlıklarını otomatik algılaması için mükemmel bir 0. satır (Header) koyuyoruz.
    perfect_rows = [["İşlem Türü", "Ücret"]] 
    raw_text = ""
    
    with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                raw_text += text + "\n"
            
            extracted_tables = page.extract_tables()
            for table in extracted_tables:
                for row in table:
                    # Hücrelerdeki enter boşluklarını temizle
                    clean_row = [str(cell).strip().replace("\n", " ") if cell else "" for cell in row]
                    
                    if not any(clean_row):
                        continue
                    
                    # 1. Fiyat hücresini bul: İçinde "TL" kelimesi geçen ve rakam barındıran ilk hücre
                    price_col_idx = -1
                    for i, cell in enumerate(clean_row):
                        if "TL" in cell.upper() and any(c.isdigit() for c in cell):
                            price_col_idx = i
                            break
                    
                    # Eğer fiyat kolonunu bulduysak
                    if price_col_idx > 0:
                        # Fiyatın solunda kalan bütün hücreleri birleştirip isim yap (bölünmüş sütunları onarır)
                        name = " ".join(clean_row[:price_col_idx]).strip()
                        price = clean_row[price_col_idx]
                        if len(name) > 3:
                            perfect_rows.append([name, price])
                            
                    # Yedek Plan: "TL" kelimesi yoksa bile, en az 2 hücre varsa ve son hücre sayılardan oluşuyorsa
                    elif len(clean_row) >= 2:
                        name = " ".join(clean_row[:-1]).strip()
                        price = clean_row[-1]
                        if any(c.isdigit() for c in price) and len(name) > 3 and not name.lower().startswith("analiz"):
                            perfect_rows.append([name, price])

    # Ayıklanan kusursuz satırları tek bir tablo objesi haline getirip gönderiyoruz.
    tables = [perfect_rows]

    # Hata ayıklama (Debug) için kaç analiz yakaladığımızı ekrana basıyoruz
    print(f"  [HÜNİTEK] Akıllı okuma tamamlandı. {len(perfect_rows)-1} adet analiz çıkarıldı.")

    return {
        "center_id": center["id"],
        "url":       pdf_url,
        "tables":    tables,
        "raw_text":  raw_text,
    }
