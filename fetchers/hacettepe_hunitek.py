"""
Fetcher: HUNITEK
Açıklama: Hacettepe Üniversitesi HÜNİTEK PDF Fiyat Listesi Çekici
"""

import io
import time
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

# PDF içindeki tabloları okumak için gerekli kütüphane
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
    last_exc = None
    
    # SSL hatalarını es geçmek için verify=False kullanıyoruz
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT, allow_redirects=True, verify=False)
            resp.raise_for_status()
            
            # Eğer indirilen şey PDF ise raw content (byte) döndür
            if is_pdf:
                return resp.content
                
            # HTML ise text döndür
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
    if pdfplumber is None:
        raise ImportError("⚠️ HATA: Bu fetcher'ı çalıştırmak için terminale 'pip install pdfplumber' yazarak PDF kütüphanesini kurmalısınız.")

    # 1. Ana sayfaya gidip güncel PDF linkini bulalım (Yıl/Ay değişse bile çalışması için)
    main_url = center.get("url", "https://hunitek.hacettepe.edu.tr/")
    
    # 2. Verdiğin direkt linki varsayılan (yedek) olarak belirleyelim
    pdf_url = "https://hunitek.hacettepe.edu.tr/wp-content/uploads/2026/01/2026_hunitek_hizmet-bedelleri.pdf"
    
    try:
        html = _get_with_retry(main_url)
        soup = BeautifulSoup(html, "html.parser")
        
        # Sayfadaki tüm linkleri tara ve hizmet bedelleri PDF'ini dinamik olarak bul
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if ".pdf" in href.lower() and "hizmet-bedelleri" in href.lower():
                pdf_url = href
                if not pdf_url.startswith("http"):
                    pdf_url = urljoin(main_url, pdf_url)
                print(f"  [HÜNİTEK] Sitede güncel PDF bulundu: {pdf_url}")
                break
    except Exception as e:
        print(f"  [HÜNİTEK] Ana sayfa taraması başarısız ({e}), yedek (doğrudan) PDF linki kullanılıyor.")

    # 3. PDF'i byte olarak indir
    pdf_content = _get_with_retry(pdf_url, is_pdf=True)
    
    tables = []
    raw_text = ""
    
    # 4. PDF'i analiz et ve tabloları çıkar
    with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
        for page in pdf.pages:
            # Yedek amaçlı ham metni (raw text) çıkar
            text = page.extract_text()
            if text:
                raw_text += text + "\n"
            
            # Tabloları çıkar (pdfplumber bu konuda çok başarılıdır)
            extracted_tables = page.extract_tables()
            for table in extracted_tables:
                clean_table = []
                for row in table:
                    # Hücrelerdeki 'None' değerleri boş stringe çevir ve satır arası boşlukları (enter) temizle
                    clean_row = [str(cell).strip().replace("\n", " ") if cell else "" for cell in row]
                    
                    # Eğer satır tamamen boş değilse dahil et
                    if any(clean_row):
                        clean_table.append(clean_row)
                
                # Tabloda en az 1 satır veri varsa listeye ekle
                if len(clean_table) > 1:
                    tables.append(clean_table)
    
    print(f"  [HÜNİTEK] PDF içinden {len(tables)} adet tablo başarıyla ayıklandı.")

    return {
        "center_id": center["id"],
        "url":       pdf_url,
        "tables":    tables,
        "raw_text":  raw_text,
    }