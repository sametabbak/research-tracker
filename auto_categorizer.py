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
    "3 Boyutlu Tarama ve Üretim": [
        "3 boyutlu", "3d", "dlp", "fdm", "polyjet",
        "yeniden yapılandırma", "modelleme", "prototip"],

    "AAS (Atomik Absorpsiyon Spektrometresi) Analizleri": [
        "aas", "atomik absorpsiyon", "alev aas", "grafit aas", "hidrür aas"],

    # MFM and KFM are AFM modes — kept in AFM, not in Elektriksel
    "AFM (Atomik Kuvvet Mikroskobu) Analizleri": [
        "afm", "atomik kuvvet", "atomic force",
        "manyetik kuvvet", "mfm", "kelvin prob", "kfm",
        "temaslı mod", "temassız mod", "contact mod", "non-contact"],

    "BET ve Yüzey Alanı (Porozimetre) Analizleri": [
        "bet", "yüzey alan", "surface area", "porozimetre", "porozimetri",
        "cıvalı", "mercury porosimetry", "adsorpsiyon", "kemisorpsiyon",
        "izoterm", "mikrogözenek"],

    "Biyolojik Numune Hazırlama": [
        "biyolojik dokulardan", "kritik nokta kurutma", "ultramikrotom",
        "kriyo-ultramikrotom", "liyofilizasyon", "dondurarak kurutma",
        "kriyostat ile kesit", "semi-dry", "immunogold"],

    "DSC (Diferansiyel Taramalı Kalorimetri)": [
        "dsc", "diferansiyel taramalı kalorimetre",
        "differential scanning calori", "tg/dsc", "tga - dsc"],

    "DTA (Diferansiyel Termal Analiz)": [
        "dta", "diferansiyel termal analiz",
        "differential thermal analys", "tg/dta"],

    "Elektriksel ve Dielektrik Ölçümler": [
        "hall etkisi", "dielektrik", "piezoelektrik",
        "elektrokimyasal", "elektrokimya", "ferroelektrik"],

    "FTIR ve IR Spektroskopi": [
        "ftir", "ft-ir", "kızılötesi", "infrared", "atr spektrum",
        "kbr pellet", "ft-fir", "nir analizi"],

    "Floresans ve Fotolüminesans Spektroskopi": [
        "floresans", "flöresans", "lüminesans",
        "fotolüminesans", "fotometrik"],

    "Genel Elementel Analizler (CHNS vb.)": [
        "chns", "elementel analiz", "eleman analizi",
        "c, h, n, s", "karbon, hidrojen"],

    "Genel Kimya ve Gıda Analizleri": [
        "aflatoksin", "alkol analiz", "asbest",
        "fitalat", "toksin", "gıda", "vitamin analizi", "yağ asit",
        "organik asit", "fenolik madde",
        "titrimetrik", "titrasyon", "katı madde tayini", "kızdırma"],

    "Genel Numune Hazırlama": [
        "numune hazırl", "örnek hazırl", "analiz çözeltisi",
        "otoklav", "homojenizatör", "rotary evaporatör",
        "reaksiyon hazırlığı", "santrifüj"],

    "Hücre Kültürü ve Biyolojik Analizler": [
        "hücre", "sitotoksisite", "mtt", "elisa", "akış sitometrisi",
        "apoptoz", "mikroplazma", "mikrobiyoloji", "biyogüvenlik",
        "caspase", "annexin", "immunogold etiket", "protein analizi",
        "protein tanımlama"],

    "ICP-MS Analizleri": [
        "icp-ms", "icp ms", "nadir toprak elementi"],

    "ICP-OES Analizleri": [
        "icp-oes", "icp oes", "icp-aes"],

    "İnce Film Kalınlık Ölçümü": [
        "ince film kalınlık", "film kalınlığı", "elipsometri"],

    "İyon Kromatografisi": [
        "anyon", "katyon", "iyon kromatografi", "ion exchange",
        "anyon, katyon"],

    "Kalori, Kömür ve Yanmazlık Testleri": [
        "kalori", "kömür", "yanmazlık", "kül tayini", "yakıt"],

    "Kaplama İşlemleri (Altın, Karbon, Platin vb.)": [
        "kaplama", "altın kaplama", "karbon kaplama",
        "paladyum kaplama", "iridyum kaplama", "sputter", "au/pd"],

    "Kimyasal Çözme ve Numune Hazırlığı": [
        "mikrodalga ile numune", "asit ile çözme", "kral suyu",
        "eritiş ile", "eritiş cihaz", "eritiş numune",
        "klasik numune hazırlık", "katı numune hazırlık",
        "mikrodalga yakma", "yaş yakma", "ase ile ekstraksiyon",
        "numune yakma", "numune hazırlığı (asit"],

    "Kromatografi (HPLC, GPC)": [
        "hplc", "preparatif hplc", "gpc", "jel geçirgenlik",
        "kantitatif: her bir numunede", "kalitatif: her bir numune"],

    "Kütle Spektrometrisi (LC-MS, GC-MS)": [
        "lc-ms", "gc-ms", "gc/ms", "lc/ms", "lc-ms/ms",
        "kütle spektrometri", "qtof", "hrms", "kütle tayini",
        "headspace", "spme", "gc-fid"],

    "Mekanik Numune Hazırlama": [
        "kırma ve öğütme", "kırma, öğütme", "kırma-öğütme",
        "kuru öğütme", "zımparalama", "parlatma",
        "presleme", "kuru presleme", "kesme",
        "soğuk kalıplama", "sıcak kalıplama", "bakalit",
        "elektro parlatma", "fiziksel öğütme",
        "mekanik numune hazırl"],

    "Mekanik Testler (Çekme, Basma, Eğme)": [
        "çekme", "basma", "eğme", "aşınma", "plastisite",
        "kopma", "mukavemet", "dinamik mekanik",
        "kalıntı gerilme", "yorulma testi"],

    "Mikrosertlik ve Makrosertlik Testleri": [
        "mikrosertlik", "sertlik", "vickers",
        "brinell", "rockwell", "knoop"],

    "Moleküler Genetik ve PCR Analizleri": [
        "pcr", "dna", "rna", "agaroz jel", "elektroforez",
        "jel yürütme", "jel görüntüleme", "jel dökümantasyon",
        "nanodrop", "real time pcr", "sds-page",
        "trans-blotlama", "protein tanımlama"],

    "NMR Analizleri": [
        "nmr", "nükleer manyetik", "cosy", "hsqc", "hmbc",
        "tocsy", "dept", "noesy", "roesy", "inadequate"],

    "Nanoindentasyon Analizleri": ["nanoindentasyon"],

    "OES (Optik Emisyon Spektrometresi) Analizleri": [
        "optik emisyon spektrometre"],

    "Optik Mikroskopi ve Görüntüleme": [
        "mikroskop", "konfokal mikroskop", "stereomikroskop",
        "inverted mikroskop", "floresan mikroskop",
        "metal mikroskobu", "görüntü analiz"],

    "Raman Spektroskopi": [
        "raman spektrum", "raman analiz", "raman ölçüm",
        "derinlik profili (1 lazer", "haritalama (1 lazer"],

    "Raporlama ve Metot Geliştirme": [
        "raporlama", "metod geliştirme", "metot geliştirme",
        "yorumlanması ve raporlama", "danışmanlık",
        "validasyon", "istatistiksel analizi"],

    # XRD and XRR are separate — different measurements
    "XRD (X-Işını Kırınım) Analizleri": [
        "xrd", "kırınım deseni", "difraksiyon",
        "x-ray difraksiyon", "saxs", "patern inceleme", "rietveld"],

    "XRF (X-Işını Floresans) Analizleri": [
        "xrf", "x-ray fluorescence", "x-ışınları floresans",
        "x-ışını floresans", "wd/xrf"],

    "XRR (X-Işını Yansıma) Analizleri": [
        "xrr", "reflectivity", "reflektivite",
        "roughness ölçümü", "yansıma analizi"],

    "XPS (X-Işını Fotoelektron Spektroskopisi) Analizleri": [
        "xps", "fotoelektron", "ups analizi",
        "açıya bağlı ölçüm", "derinlik profili (ar iyonları"],

    "Radyoaktivite Analizleri": [
        "radyoaktivite", "cs-137", "ra-226", "th-232",
        "gama ışını", "pb-210", "tarihlendirme"],

    "SEM (Tarama Elektron Mikroskobu) Görüntüleme Analizleri": [
        "sem görüntü", "fe-sem görüntü", "fe-sem ile inceleme",
        "taramalı elektron", "scanning electron",
        "katodolüminesans", "sem-cl", "sem-ebic"],

    "SEM-EDX ve EBSD Analizleri": [
        "edx", "eds analiz", "ebsd", "enerji dağılımlı",
        "sem-edx", "sem/edx", "fib", "odaklanmış iyon demeti",
        "metalik kalıntı analizi"],

    "Su Kalitesi ve Çevre Analizleri": [
        "sularda ph", "sularda elektrik", "ph tayini", "ph ölçüm",
        "elektrometrik metot", "çözünmüş oksijen",
        "toplam sertlik", "toplam çözünmüş",
        "tuzluluk", "bulanıklık"],

    "TEM ve STEM (Geçirimli Elektron Mikroskobu) Analizleri": [
        "\btem\b", "\bstem\b", "geçirimli elektron",
        "transmission electron", "kriyo-tem"],

    "TGA (Termogravimetrik Analiz)": [
        "tga", "termogravimetrik", "thermogravimetric",
        "tg/dta – analizleri", "termal gravimetrik"],

    "DSC (Diferansiyel Taramalı Kalorimetri)": [
        "dsc", "diferansiyel taramalı kalorimetre",
        "differential scanning calori", "tg/dsc", "tga - dsc"],

    "Tane Boyutu ve Zeta Potansiyeli Analizleri": [
        "tane boyut", "zeta", "partikül boyut",
        "parçacık boyut", "elek analizi", "lazer tekniği ile"],

    "Temas Açısı ve Yüzey Enerjisi Analizleri": [
        "temas açısı", "yüzey gerilimi", "serbest yüzey enerjisi",
        "ıslanabilirlik", "yüzey serbest enerji"],

    "Termal Analizler (STA, DMA, TMA)": [
        "\bsta\b", "dinamik mekanik analiz", "\bdma\b", "\btma\b",
        "termal mekanik", "ısı kapasitesi",
        "sıcaklık taraması", "ısıl geçirgenlik"],

    "UV-VIS Spektrofotometri": [
        "uv-vis", "uv vis", "uvvis", "uv-vis-nir",
        "absorbans", "transmittans", "spektrofotometre"],

    "Yoğunluk ve Fiziksel Özellikler": [
        "yoğunluk", "piknometre", "özgül ağırlık", "viskozite"],

    "Yüzey Pürüzlülüğü ve Profilometri": [
        "pürüzlülük", "profilometre", "yüzey profili",
        "topografik", "yüzey porozitesi"],

    # Training is intentionally excluded — training fees ≠ analysis fees
    # They go to "Diğer" fallback
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
