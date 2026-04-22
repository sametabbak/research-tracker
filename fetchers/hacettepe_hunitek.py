"""
Fetcher: HACETTEPE_HUNITEK
Açıklama: Hacettepe Üniversitesi HÜNİTEK PDF (Deney -> Kategori, Tanım -> Analiz, Bedel -> Fiyat)
"""

import io
import re
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

def split_cell(text):
    """Hücre içindeki alt alta yazılmış analizleri (PCR örneğindeki gibi) listeye böler."""
    if not text: return []
    # Tek enter'ları boşluk yap, 2 veya daha fazla enter'ı ayırma noktası kabul et
    text = re.sub(r'(?<!\n)\n(?!\n)', ' ', text)
    return [line.strip() for line in re.split(r'\n{2,}', text) if line.strip()]

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
    
    # Sistemin ana kuralı: Çıktı kesinlikle bu 3 sütunlu düzende olacak
    perfect_rows = [["Kategori", "İşlem Türü", "Ücret"]] 
    raw_text = ""
    
    with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
        last_category = "Genel Analizler"
        
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                raw_text += text + "\n"
            
            extracted_tables = page.extract_tables()
            for table in extracted_tables:
                for row in table:
                    if not row or not any(row):
                        continue
                        
                    str_row = [str(cell) if cell is not None else "" for cell in row]
                    
                    # PDF'teki tablo tam olarak 3 sütunlu mu kontrol et
                    if len(str_row) >= 3:
                        cat_raw = str_row[0].strip()
                        tanim_raw = str_row[1].strip()
                        fiyat_raw = str_row[-1].strip() # Son sütun her zaman fiyat
                        
                        # Eğer bu bir başlık satırıysa (Deney/Metot/Cihaz yazıyorsa) atla
                        if "DENEY" in cat_raw.upper() or "METOT" in cat_raw.upper():
                            continue
                            
                        # 1. SÜTUN: Deney/Metot/Cihaz (Kategori)
                        if cat_raw:
                            # Kategori hücresi doluysa, hafızayı güncelle
                            last_category = " ".join([line.strip() for line in cat_raw.split('\n') if line.strip()])
                            
                        # 2. ve 3. SÜTUN: Tanım ve Hizmet Bedeli
                        tanim_lines = split_cell(tanim_raw)
                        fiyat_lines = split_cell(fiyat_raw)
                        
                        # Hücre içinde birden fazla analiz alt alta yazılmışsa (PCR tablosu gibi)
                        if len(tanim_lines) == len(fiyat_lines) and len(tanim_lines) > 0:
                            for t, f in zip(tanim_lines, fiyat_lines):
                                if any(c.isdigit() for c in f): # Fiyat sütununda rakam varsa ekle
                                    perfect_rows.append([last_category, t, f])
                        else:
                            # Tek satırlık normal bir analizse
                            t = " ".join(tanim_lines)
                            f = " ".join(fiyat_lines)
                            if len(t) > 2 and any(c.isdigit() for c in f):
                                perfect_rows.append([last_category, t, f])
                                
                    # Tablo yapısı 3 sütundan azsa ama fiyat içeriyorsa (bazı hatalı PDF satırları için kurtarma)
                    elif len(str_row) == 2:
                        tanim_raw = str_row[0].strip()
                        fiyat_raw = str_row[1].strip()
                        if "TANIM" not in tanim_raw.upper() and any(c.isdigit() for c in fiyat_raw):
                            perfect_rows.append([last_category, tanim_raw.replace('\n', ' '), fiyat_raw.replace('\n', ' ')])

    tables = [perfect_rows]

    print(f"  [HÜNİTEK] 'Deney -> Kategori, Tanım -> İşlem' eşleştirmesiyle {len(perfect_rows)-1} adet analiz çekildi.")

    return {
        "center_id": center["id"],
        "url":       pdf_url,
        "tables":    tables,
        "raw_text":  raw_text,
    }
