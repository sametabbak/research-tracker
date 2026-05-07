import json
import os
import glob
import difflib
import re

# Dosya yolları
KEYWORDS_FILE = "data/keywords.json"
ANALYSES_DIR = "data/analyses/*.json"

# YENİ SİSTEM: Semantik kategori yapısına uygun arama motoru (Keyword Mapping)
CATEGORY_KEYWORDS = {
    "3 Boyutlu Tarama ve Üretim": ["3 boyutlu", "3d", "tarama", "modelleme", "dlp", "fdm", "polyjet", "yeniden yapılandırma"],
    "AAS (Atomik Absorpsiyon Spektrometresi) Analizleri": ["aas", "atomik absorpsiyon"],
    "AFM (Atomik Kuvvet Mikroskobu) Analizleri": ["afm", "atomik kuvvet", "atomic force"],
    "BET ve Yüzey Alanı (Porozimetre) Analizleri": ["bet", "yüzey alan", "surface area", "porozimetre", "porozimetri", "cıvalı", "mercury porosimetry", "adsorpsiyon", "kemisorpsiyon"],
    "Elektriksel, Manyetik ve Dielektrik Ölçümler": ["dielektrik", "piezoelektrik", "hall etkisi", "iletkenlik", "manyetik kuvvet", "kfm", "elektrokimyasal"],
    "Eğitim, Danışmanlık ve Raporlama": ["eğitim", "raporlama", "danışmanlık", "metod geliştirme", "metot geliştirme"],
    "FTIR, Raman ve Optik Spektroskopi": ["ftir", "ft-ir", "kızılötesi", "infrared", "raman", "lüminesans", "spektrometri", "absorbans", "spektrum", "optik emisyon"],
    "Genel Elementel Analizler (CHNS vb.)": ["chns", "elementel analiz", "eleman analizi"],
    "Genel Kimya, Su, Çevre ve Gıda Analizleri": ["kükürt", "aflatoksin", "alkol", "anyon", "katyon", "asbest", "bulanıklık", "ph", "fitalat", "pestisit", "toksin", "su", "gıda", "titrasyon", "şeker", "nem", "çözünmüş"],
    "Genel Numune Hazırlama İşlemleri (Kesme, Öğütme, Çözme)": ["numune hazırl", "izostatik", "kesme", "öğütme", "çözündürme", "mikrodalga yakma", "kriyostat", "liyofilizasyon", "santrifüj", "otoklav", "homojenizatör", "eritiş", "zımpara", "parlatma", "soğuk kalıplama"],
    "Hücre Kültürü ve Biyolojik Analizler": ["hücre", "sitotoksisite", "mtt", "elisa", "akış sitometrisi", "apoptoz", "protein", "mikroplazma"],
    "ICP (İndüktif Eşleşmiş Plazma) Analizleri": ["icp-ms", "icp ms", "icp-oes", "icp"],
    "Kalori, Kömür ve Yanmazlık Testleri": ["kalori", "kömür", "yanmazlık", "kül"],
    "Kaplama İşlemleri (Altın, Karbon, Platin vb.)": ["kaplama", "altın kaplama", "karbon kaplama", "paladyum", "iridyum", "sputter"],
    "Kromatografi (HPLC, GC, GPC)": ["hplc", "gc", "gpc", "kromatografi"],
    "Kütle Spektrometrisi (LC-MS, GC-MS)": ["lc-ms", "gc-ms", "kütle spektrometri", "lc/ms", "gc/ms", "qtof", "hrms"],
    "Mekanik Testler (Çekme, Basma, Eğme)": ["çekme", "basma", "eğme", "aşınma", "plastisite", "kopma", "mukavemet", "dinamik mekanik", "kalıntı gerilme"],
    "Mikrosertlik ve Makrosertlik Testleri": ["mikrosertlik", "sertlik", "vickers", "brinell", "rockwell", "knoop"],
    "Moleküler Genetik ve PCR Analizleri": ["pcr", "dna", "rna", "agaroz", "elektroforez", "genetik", "jel"],
    "NMR Analizleri": ["nmr", "nükleer manyetik", "cosy", "hsqc", "hmbc", "tocsy", "dept"],
    "Nanoindentasyon": ["nanoindentasyon"],
    "OES (Optik Emisyon Spektrometresi) Analizleri": ["oes", "optik emisyon"],
    "Optik Mikroskopi ve Görüntüleme": ["mikroskop", "floresan", "konfokal", "stereomikroskop", "görüntüleme", "görüntü alma"],
    "SEM (Tarama Elektron Mikroskobu) Görüntüleme Analizleri": ["sem", "taramalı elektron", "scanning electron", "fe-sem", "katodolüminesans"],
    "SEM-EDX ve EBSD Analizleri": ["edx", "eds", "ebsd", "enerji dağılımlı", "haritalama"],
    "TEM ve STEM (Geçirimli Elektron Mikroskobu) Analizleri": ["tem", "stem", "geçirimli elektron", "transmission electron", "kriyo-tem", "ultramikrotom"],
    "Tane Boyutu ve Zeta Potansiyeli Analizleri": ["tane boyut", "zeta", "partikül", "elek analizi"],
    "Termal Analizler (TGA, DSC, DTA, STA)": ["tga", "dsc", "dta", "sta", "termogravimetrik", "diferansiyel taramalı", "ısı kapasitesi", "termal"],
    "XPS (X-Işını Fotoelektron Spektroskopisi) Analizleri": ["xps", "fotoelektron"],
    "XRD (X-Işını Kırınım) ve XRR Analizleri": ["xrd", "xrr", "kırınım", "difraksiyon", "x-ray", "x ışını", "saxs", "patern"],
    "XRF (X-Işını Floresans) Analizleri": ["xrf", "floresans", "x-ray fluorescence"],
    "Yüzey Gerilimi, Temas Açısı ve Fiziksel Ölçümler": ["yüzey gerilimi", "temas açısı", "viskozite", "yoğunluk", "piknometre", "pürüzlülük", "profilometre", "kalınlık", "fib", "özgül ağırlık"]
}

def load_json(filepath):
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_json(data, filepath):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def get_all_known_analyses(keywords_data):
    known = set()
    for category, items in keywords_data.items():
        for item in items:
            known.add(item.strip())
    return known

def get_all_fetched_analyses():
    fetched = set()
    for file in glob.glob(ANALYSES_DIR):
        data = load_json(file)
        for analysis in data.get("analyses", []):
            if "name" in analysis:
                fetched.add(analysis["name"].strip())
    return fetched

def auto_categorize():
    keywords_data = load_json(KEYWORDS_FILE)
    
    # Bilinmeyenler için ana havuz
    fallback_category = "Diğer Analiz ve Laboratuvar Hizmetleri"
    if fallback_category not in keywords_data:
        keywords_data[fallback_category] = []
        
    known_analyses = get_all_known_analyses(keywords_data)
    fetched_analyses = get_all_fetched_analyses()

    new_analyses = fetched_analyses - known_analyses

    if not new_analyses:
        print("Sınıflandırılacak yeni analiz bulunamadı.")
        return

    print(f"{len(new_analyses)} adet yeni analiz tespit edildi. Sınıflandırma başlıyor...")

    for analysis in new_analyses:
        categorized = False
        analysis_lower = analysis.lower()

        # 1. YÖNTEM: Akıllı Kelime Eşleştirme (Keyword Mapping)
        for category, kws in CATEGORY_KEYWORDS.items():
            if any(kw in analysis_lower for kw in kws):
                if category not in keywords_data:
                    keywords_data[category] = []
                keywords_data[category].append(analysis)
                print(f"[BAŞARILI] '{analysis}' -> '{category}' kategorisine eklendi.")
                categorized = True
                break
        
        if categorized:
            continue

        # 2. YÖNTEM: Metin Benzerliği (Fuzzy Matching)
        best_match = None
        best_ratio = 0.0
        best_category = None

        for category, items in keywords_data.items():
            for item in items:
                ratio = difflib.SequenceMatcher(None, analysis_lower, item.lower()).ratio()
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_match = item
                    best_category = category

        if best_ratio > 0.70:
            keywords_data[best_category].append(analysis)
            print(f"[BENZERLİK] '{analysis}' -> '{best_category}' kategorisine eklendi. (Eşleşen: {best_match})")
            categorized = True
            continue

        # 3. YÖNTEM: Rastgele kategori açmak yerine güvenli havuza (Diğer) atma
        keywords_data[fallback_category].append(analysis)
        print(f"[DİĞER] '{analysis}' -> '{fallback_category}' kategorisine eklendi.")

    # JSON dosyasını alfabetik ve temiz bir düzende kaydetme
    for cat in keywords_data:
        keywords_data[cat] = sorted(list(set(keywords_data[cat])))
    
    sorted_keywords_data = {k: keywords_data[k] for k in sorted(keywords_data.keys())}
    save_json(sorted_keywords_data, KEYWORDS_FILE)
    print("keywords.json başarıyla güncellendi!")

if __name__ == "__main__":
    auto_categorize()
