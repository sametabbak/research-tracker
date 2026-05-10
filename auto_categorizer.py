"""
auto_categorizer.py
───────────────────
Assigns a keyword_group to every analysis entry in data/analyses/*.json
that does not already have one.

Key design decisions:
  - Keyword matching only. Fuzzy matching removed entirely — it caused
    generic short names to contaminate specific technical groups.
  - Once an analysis has a keyword_group (set by this script or by a human
    in the review panel), it is never overwritten.
  - The Diğer fallback group is no longer written to keyword_group; those
    entries stay null and appear in the panel's Sınıflandırma tab.
  - keywords.json is now a GROUP DEFINITION FILE only (group name → keyword
    triggers). It no longer stores lists of analysis names.
"""

import json
import re
import glob
from pathlib import Path

KEYWORDS_FILE  = Path("data/keywords.json")
ANALYSES_GLOB  = "data/analyses/*.json"


# ── Group definitions ─────────────────────────────────────────────────────────
# These keywords are used ONLY to match new, unassigned analyses.
# They do not affect analyses that already have a keyword_group.
# Use r"\bword\b" for whole-word matching, plain string for substring.

CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "3 Boyutlu Tarama ve Üretim": [
        "3 boyutlu", "3d tarama", "3d baskı", "dlp", "fdm", "polyjet",
        "yeniden yapılandırma", "tersine mühendislik"],

    "AAS (Atomik Absorpsiyon Spektrometresi) Analizleri": [
        r"\baas\b", "atomik absorpsiyon", "alev aas", "grafit aas", "hidrür aas"],

    "AFM (Atomik Kuvvet Mikroskobu) Analizleri": [
        r"\bafm\b", "atomik kuvvet", "atomic force",
        "manyetik kuvvet", r"\bmfm\b", "kelvin prob", r"\bkfm\b",
        "temaslı mod", "temassız mod", "contact mod", "non-contact mod"],

    "BET ve Yüzey Alanı (Porozimetre) Analizleri": [
        r"\bbet\b", "yüzey alan", "surface area",
        "porozimetre", "porozimetri", "cıvalı porozim",
        "mercury porosimetry", "adsorpsiyon izotemi",
        "kemisorpsiyon", "mikrogözenek"],

    "Biyolojik Numune Hazırlama": [
        "biyolojik dokulardan", "kritik nokta kurutma",
        "kriyo-ultramikrotom", "kriyostat ile kesit",
        "liyofilizasyon", "dondurarak kurutma", "immunogold"],

    "DSC (Diferansiyel Taramalı Kalorimetri)": [
        r"\bdsc\b", "diferansiyel taramalı kalorimetre",
        "differential scanning calori", r"\btg/dsc\b"],

    "DTA (Diferansiyel Termal Analiz)": [
        r"\bdta\b", "diferansiyel termal analiz",
        "differential thermal analys", r"\btg/dta\b"],

    "Elektriksel ve Dielektrik Ölçümler": [
        "hall etkisi", "dielektrik ölçüm", "piezoelektrik",
        "elektrokimyasal", "ferroelektrik"],

    "FTIR ve IR Spektroskopi": [
        r"\bftir\b", r"\bft-ir\b", "kızılötesi spektrum",
        "infrared spektrum", "atr spektrum", r"\bft-fir\b"],

    "Floresans ve Fotolüminesans Spektroskopi": [
        "floresans ölçüm", "flöresans ölçüm",
        "fotolüminesans", "lüminesans ölçüm"],

    "Genel Elementel Analizler (CHNS vb.)": [
        r"\bchns\b", "elementel analiz", "eleman analizi",
        "c, h, n, s analizi"],

    "Genel Kimya ve Gıda Analizleri": [
        "aflatoksin", "asbest analizi", "fitalat",
        "gıda analizi", "pestisit analizi",
        "organik asit analizi", "fenolik madde tayini",
        "titrimetrik", "toplam fenolik"],

    "Genel Numune Hazırlama": [
        "numune hazırl", "örnek hazırl",
        "otoklav kullanım", "homojenizatör", "santrifüj"],

    "Hücre Kültürü ve Biyolojik Analizler": [
        "hücre kültür", "sitotoksisite", r"\bmtt\b",
        r"\belisa\b", "akış sitometrisi", "apoptoz",
        "mikroplazma testi", "mikrobiyoloji", "biyogüvenlik"],

    "ICP-MS Analizleri": [
        r"\bicp-ms\b", r"\bicp ms\b", "nadir toprak elementi"],

    "ICP-OES Analizleri": [
        r"\bicp-oes\b", r"\bicp oes\b", r"\bicp-aes\b"],

    "İnce Film Kalınlık Ölçümü": [
        "ince film kalınlık", "film kalınlığı", "elipsometri"],

    "İyon Kromatografisi": [
        "anyon analizi", "katyon analizi",
        "iyon kromatografi", "anyon, katyon"],

    "Kalori, Kömür ve Yanmazlık Testleri": [
        "kalori tayini", "kömür analizi", "yanmazlık",
        "kül tayini", "yakıt analizi", "nem tayini"],

    "Kaplama İşlemleri (Altın, Karbon, Platin vb.)": [
        "altın kaplama", "karbon kaplama",
        "paladyum kaplama", "iridyum kaplama",
        r"\bsputter\b", "au/pd kaplama", "numune kaplama"],

    "Kimyasal Çözme ve Numune Hazırlığı": [
        "mikrodalga ile numune",
        "asit ile çözme", "kral suyu",
        "eritiş ile çözme", "mikrodalga yakma",
        "yaş yakma", "ase ile ekstraksiyon"],

    "Kromatografi (HPLC, GPC)": [
        r"\bhplc\b", "preparatif hplc",
        r"\bgpc\b", "jel geçirgenlik kromatografi"],

    "Kütle Spektrometrisi (LC-MS, GC-MS)": [
        r"\blc-ms\b", r"\bgc-ms\b", r"\bgc/ms\b", r"\blc/ms\b",
        r"\bhrms\b", r"\bqtof\b", "kütle spektrometri", r"\bgc-fid\b"],

    "Mekanik Numune Hazırlama": [
        "kırma ve öğütme", "kuru öğütme",
        "zımparalama", "elektro parlatma",
        "soğuk kalıplama", "sıcak kalıplama", r"\bbakalit\b",
        "mekanik numune hazırl"],

    "Mekanik Testler (Çekme, Basma, Eğme)": [
        "çekme testi", "basma testi", "eğme testi",
        "aşınma testi", "kopma mukavemeti",
        "dinamik mekanik analiz", "kalıntı gerilme",
        "yorulma testi", "üç nokta eğme"],

    "Mikrosertlik ve Makrosertlik Testleri": [
        "mikrosertlik", r"\bvickers\b",
        r"\bbrinell\b", r"\brockwell\b", r"\bknoop\b"],

    "Moleküler Genetik ve PCR Analizleri": [
        r"\bpcr\b", "dna izolasyon", "rna izolasyon",
        "agaroz jel yürütme", "jel elektroforez",
        "jel görüntüleme", "jel dökümantasyon",
        r"\bnanodrop\b", "real time pcr", r"\bsds-page\b",
        "trans-blotlama", "protein tanımlama"],

    "NMR Analizleri": [
        r"\bnmr\b", "nükleer manyetik",
        r"\bcosy\b", r"\bhsqc\b", r"\bhmbc\b",
        r"\btocsy\b", r"\bdept\b", r"\bnoesy\b", r"\broesy\b"],

    "Nanoindentasyon Analizleri": [r"\bnanoindentasyon\b"],

    "OES (Optik Emisyon Spektrometresi) Analizleri": [
        "optik emisyon spektrometre", "1-optik emisyon"],

    "Optik Mikroskopi ve Görüntüleme": [
        "ışık mikroskobu", "konfokal mikroskop",
        "stereomikroskop", "inverted mikroskop",
        "floresan mikroskop", "metal mikroskobu",
        "görüntü analiz sistemi", "hazır preparat görüntüleme",
        "görüntü alma", "görüntüleme"],

    "Raman Spektroskopi": [
        "raman spektrum", "raman analiz", "raman ölçüm"],

    "Raporlama ve Metot Geliştirme": [
        "analiz sonuçlarının yorumlanması",
        "metot geliştirme", "metod geliştirme",
        "istatistiksel analizi ve yorumlanması"],

    "SEM (Tarama Elektron Mikroskobu) Görüntüleme Analizleri": [
        r"\bsem\b görüntü", r"\bsem\b analizi",
        r"\bsem\b cihaz", "fe-sem görüntü",
        "fe-sem ile inceleme", "taramalı elektron",
        "scanning electron", r"\bsem-cl\b",
        "katodolüminesans", r"\bsem-ebic\b",
        "yüzey görüntüsü alma"],

    "SEM-EDX ve EBSD Analizleri": [
        r"\bedx\b", r"\beds\b", r"\bebsd\b",
        "enerji dağılımlı x", "sem-edx", "sem/edx",
        r"\bfib\b", "odaklanmış iyon demeti",
        "eds modu", "edx modu", "haritalama",
        "metalik kalıntı analizi"],

    "Su Kalitesi ve Çevre Analizleri": [
        "sularda ph", "elektrometrik metot ile ph",
        "çözünmüş oksijen tayini", "toplam sertlik tayini",
        "toplam çözünmüş madde", "bulanıklık tayini"],

    "TEM ve STEM (Geçirimli Elektron Mikroskobu) Analizleri": [
        r"\btem\b analiz", r"\bstem\b analiz",
        "geçirimli elektron", "transmission electron",
        r"\bkriyo-tem\b", "biyolojik dokulardan tem"],

    "TGA (Termogravimetrik Analiz)": [
        r"\btga\b", "termogravimetrik", "thermogravimetric",
        "termal gravimetrik"],

    "Tane Boyutu ve Zeta Potansiyeli Analizleri": [
        "tane boyutu", "partikül boyut", "parçacık boyut",
        "zeta potansiyeli", "elek analizi",
        "lazer kırınım", "dinamik ışık saçılması"],

    "Temas Açısı ve Yüzey Enerjisi Analizleri": [
        "temas açısı", "yüzey gerilimi ölçüm",
        "serbest yüzey enerjisi", "ıslanabilirlik"],

    "Termal Analizler (STA, DMA, TMA)": [
        r"\bsta\b analiz", "dinamik mekanik analiz",
        r"\bdma\b analiz", r"\btma\b analiz",
        "ısıl geçirgenlik", "ısı kapasitesi ölçüm"],

    "UV-VIS Spektrofotometri": [
        "uv-vis", "uv vis spektrum", "uv-vis-nir",
        "spektrofotometre", "absorbans ölçüm", "transmittans"],

    "XPS (X-Işını Fotoelektron Spektroskopisi) Analizleri": [
        r"\bxps\b", "fotoelektron spektroskopi",
        "ups analizi", "açıya bağlı xps"],

    "XRD (X-Işını Kırınım) Analizleri": [
        r"\bxrd\b", "x-ışını kırınım", "difraksiyon deseni",
        r"\bsaxs\b", "patern inceleme", "rietveld analiz",
        "kalitatif mineral analiz"],

    "XRF (X-Işını Floresans) Analizleri": [
        r"\bxrf\b", "x-ışını floresans", "x-ray fluorescence",
        r"\bwd/xrf\b"],

    "XRR (X-Işını Yansıma) Analizleri": [
        r"\bxrr\b", "reflectivity", "reflektivite",
        "x-ışını yansıma"],

    "Yoğunluk ve Fiziksel Özellikler": [
        "yoğunluk tayini", "özgül ağırlık tayini",
        "piknometre", "viskozite ölçüm"],

    "Yüzey Pürüzlülüğü ve Profilometri": [
        "yüzey pürüzlülüğü", "profilometre",
        "3b yüzey profili", "topografik ölçüm"],
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_json(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def save_json(data: dict, path: Path) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def kw_matches(keyword: str, text: str) -> bool:
    """
    Match a keyword against lowercased analysis text.
    Keywords wrapped in \\b use regex word-boundary matching.
    All others use plain substring matching.
    """
    if "\\b" in keyword:
        return bool(re.search(keyword, text, re.IGNORECASE))
    return keyword.lower() in text


def find_group(name: str) -> str | None:
    """
    Return the first matching group for an analysis name, or None.
    No fuzzy matching — keyword matching only.
    """
    name_lower = name.lower()
    for group, keywords in CATEGORY_KEYWORDS.items():
        if any(kw_matches(kw, name_lower) for kw in keywords):
            return group
    return None


# ── Main ──────────────────────────────────────────────────────────────────────

def auto_categorize() -> None:
    total_assigned = 0
    total_skipped  = 0
    total_no_match = 0

    for path_str in sorted(glob.glob(ANALYSES_GLOB)):
        path = Path(path_str)
        data = load_json(path)
        analyses = data.get("analyses", [])
        changed = 0

        for entry in analyses:
            name = entry.get("name", "").strip()
            if not name:
                continue

            # Never overwrite an existing human or auto assignment
            if entry.get("keyword_group"):
                total_skipped += 1
                continue

            group = find_group(name)
            if group:
                entry["keyword_group"] = group
                changed += 1
                total_assigned += 1
                print(f"  [OK] '{name[:55]}' → '{group}'")
            else:
                entry["keyword_group"] = None
                total_no_match += 1

        if changed:
            save_json(data, path)

    print(f"\nTamamlandı.")
    print(f"  Yeni atama    : {total_assigned}")
    print(f"  Zaten atanmış : {total_skipped}")
    print(f"  Eşleşme yok  : {total_no_match}  ← panel Sınıflandırma sekmesinden atayın")


if __name__ == "__main__":
    auto_categorize()
