import json
import os
import glob
import difflib
import re

# Dosya yolları
KEYWORDS_FILE = "data/keywords.json"
ANALYSES_DIR = "data/analyses/*.json"

def load_json(filepath):
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_json(data, filepath):
    with open(filepath, "w", encoding="utf-8") as f:
        # indent=2 ve ensure_ascii=False ile formatın bozulmasını engelliyoruz
        json.dump(data, f, indent=2, ensure_ascii=False)

def get_all_known_analyses(keywords_data):
    """Halihazırda keywords.json içinde olan tüm analizleri bir Set olarak döndürür."""
    known = set()
    for category, items in keywords_data.items():
        for item in items:
            known.add(item.strip())
    return known

def get_all_fetched_analyses():
    """Üniversitelerden çekilen güncel analiz isimlerini toplar."""
    fetched = set()
    for file in glob.glob(ANALYSES_DIR):
        data = load_json(file)
        # Eğer analizler dict listesi ise (örn: {"name": "XRD...", "price": ...})
        for analysis in data.get("analyses", []):
            if "name" in analysis:
                fetched.add(analysis["name"].strip())
    return fetched

def auto_categorize():
    keywords_data = load_json(KEYWORDS_FILE)
    known_analyses = get_all_known_analyses(keywords_data)
    fetched_analyses = get_all_fetched_analyses()

    # Sadece keywords.json'da OLMAYAN yeni analizleri bul
    new_analyses = fetched_analyses - known_analyses

    if not new_analyses:
        print("Sınıflandırılacak yeni analiz bulunamadı.")
        return

    print(f"{len(new_analyses)} adet yeni analiz tespit edildi. Sınıflandırma başlıyor...")

    for analysis in new_analyses:
        categorized = False
        analysis_lower = analysis.lower()

        # 1. YÖNTEM: Kelime İçerme (Keyword Matching)
        # Eğer yeni analizin adında ana kategori ismi geçiyorsa (Örn: adında 'SEM' geçiyorsa)
        for category in keywords_data.keys():
            if category.lower() in analysis_lower:
                keywords_data[category].append(analysis)
                print(f"[BAŞARILI] '{analysis}' -> '{category}' kategorisine eklendi (Kelime Eşleşmesi).")
                categorized = True
                break
        
        if categorized:
            continue

        # 2. YÖNTEM: Metin Benzerliği (Fuzzy Matching)
        # İçinde geçmiyor ama harf hataları/ekleri varsa
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

        # Eğer %70'ten fazla benzerlik varsa o kategoriye ata
        if best_ratio > 0.70:
            keywords_data[best_category].append(analysis)
            print(f"[BENZERLİK] '{analysis}' -> '{best_category}' kategorisine eklendi. (Eşleşen: {best_match})")
            categorized = True
            continue

        # 3. YÖNTEM: Yeni Kategori Oluşturma (Fallback)
        # Hiçbir kategoriye uymuyorsa, ismindeki ilk anlamlı kelimeyi alıp kategori yapar
        # "1-" gibi sıra numaralarını temizle
        clean_name = re.sub(r"^\d+[-\s]+", "", analysis)
        words = clean_name.split()
        
        if words:
            # Sadece ilk kelimeyi al (Örn: "Porozimetre Analizi" -> "Porozimetre")
            new_category = words[0].strip("()-,*")
            if len(new_category) > 2: # "ve", "ile" gibi kısa kelimeleri engellemek için
                if new_category not in keywords_data:
                    keywords_data[new_category] = []
                keywords_data[new_category].append(analysis)
                print(f"[YENİ KATEGORİ] '{analysis}' için '{new_category}' kategorisi oluşturuldu.")
            else:
                # Çok kısaysa "Diğer" kategorisine at
                if "Diğer Sınıflandırılmayanlar" not in keywords_data:
                    keywords_data["Diğer Sınıflandırılmayanlar"] = []
                keywords_data["Diğer Sınıflandırılmayanlar"].append(analysis)
                print(f"[DİĞER] '{analysis}' -> 'Diğer Sınıflandırılmayanlar' kategorisine eklendi.")

    # İşlem bitince dosyayı güvenle kaydet
    save_json(keywords_data, KEYWORDS_FILE)
    print("keywords.json başarıyla güncellendi!")

if __name__ == "__main__":
    auto_categorize()
