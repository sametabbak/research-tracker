"""
Fetcher: HACETTEPE_HUNITEK
Açıklama: Hacettepe Üniversitesi HÜNİTEK PDF Fiyat Listesi Çekici (Regex Destekli Akıllı Hücre Bölme ve Kategori Hafızası)
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

def split_cell(text):
    """
    pdfplumber'dan gelen karmaşık hücre metnini akıllıca böler.
    Tekli \n (satır kayması) boşluğa çevrilir, çoklu \n (farklı analizler) listeye bölünür.
    """
    if not text: return []
    # Tekli \n karakterlerini (öncesinde ve sonrasında \n olmayanları) boşluğa çevir
    text = re.sub(r'(?<!\n)\n(?!\n)', ' ', text)
    # Geriye kalan \n (iki veya daha fazla) üzerinden listeye böl
    lines = [line.strip() for line in re.split(r'\n+', text) if line.strip()]
    return lines

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
    
    # json_exporter'ın kusursuz okuması için standart başlıklar
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
                        
                    # Boş (None) hücreleri string yap
                    str_row = [str(cell) if cell is not None else "" for cell in row]
                    
                    # 1. Fiyat sütununu tespit et (İçinde TL ve rakam olan son sütun)
                    price_col_idx = -1
                    for i, cell in enumerate(str_row):
                        if "TL" in cell.upper() and any(c.isdigit() for c in cell):
                            price_col_idx = i
                            
                    if price_col_idx == -1:
                        if len(str_row) >= 2 and any(c.isdigit() for c in str_row[-1]):
                            price_col_idx = len(str_row) - 1
                        else:
                            continue
                            
                    # 2. Kategori Hafızası Güncellemesi (Her zaman İlk Sütun)
                    if price_col_idx >= 2:
                        cat_raw = str_row[0].strip()
                        if cat_raw and "DENEY" not in cat_raw.upper():
                            # Kategori adındaki gereksiz alt satıra geçmeleri boşlukla birleştirip tek isim yap
                            last_category = " ".join([line.strip() for line in cat_raw.split('\n') if line.strip()])
                            
                    # 3. Tanım ve Fiyat verisini al
                    tanim_raw = " ".join(str_row[1:price_col_idx]) if price_col_idx >= 2 else str_row[0]
                    fiyat_raw = str_row[price_col_idx]
                    
                    # Regex tabanlı akıllı bölme işlemi
                    tanim_lines = split_cell(tanim_raw)
                    fiyat_lines = split_cell(fiyat_raw)
                    
                    # Eğer Tanım satır sayısı ile Fiyat satır sayısı tam eşleşiyorsa (örneğin 3 tanım, 3 fiyat)
                    if len(tanim_lines) == len(fiyat_lines) and len(tanim_lines) > 0:
                        for t, f in zip(tanim_lines, fiyat_lines):
                            if len(t) > 2 and "TANIM" not in t.upper():
                                perfect_rows.append([last_category, t, f])
                    else:
                        # Eşleşmiyorsa veri kaybını önlemek için güvenli şekilde birleştirip tek satır yap
                        t = " ".join(tanim_lines)
                        f = " ".join(fiyat_lines)
                        if len(t) > 2 and "TANIM" not in t.upper():
                            perfect_rows.append([last_category, t, f])

    tables = [perfect_rows]

    print(f"  [HÜNİTEK] Regex destekli akıllı okuma tamamlandı. {len(perfect_rows)-1} adet analiz çıkarıldı.")

    return {
        "center_id": center["id"],
        "url":       pdf_url,
        "tables":    tables,
        "raw_text":  raw_text,
    }
