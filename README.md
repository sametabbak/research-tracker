# AnalizBul — Veri Deposu (Herkese Açık)

Bu depo iki şey içerir:
1. **Fiyat takip sistemi** — Türkiye'deki araştırma merkezlerinin analiz ücret sayfalarını otomatik olarak izler
2. **Yapılandırılmış JSON verileri** — AnalizBul mobil uygulamasının okuduğu dosyalar

---

## Depo Yapısı

```
analizbul-data/
  ├── tracker.py                  ← Ana çalıştırıcı
  ├── json_exporter.py            ← Uygulama için JSON çıktısı
  ├── diff_engine.py              ← Anlık görüntü karşılaştırma
  ├── notifier.py                 ← Değişiklik bildirimleri
  ├── requirements.txt
  │
  ├── config/
  │   └── centers.json            ← Merkez profilleri ve ayarları
  │
  ├── fetchers/
  │   ├── html_table.py           ← HTML tablo kazıyıcı (DPÜ İLTEM, ESOGÜ ARUM)
  │   ├── html_then_pdf.py        ← Dinamik PDF bağlantı + PDF okuyucu (AKÜ TUAM)
  │   └── manual.py              ← Manuel güncelleme hatırlatıcısı
  │
  ├── snapshots/                  ← Son bilinen sayfa durumları (otomatik güncellenir)
  │   ├── aku_tuam.json
  │   ├── esogu_arum.json
  │   └── dpu_iltem.json
  │
  └── data/                       ← 📱 Mobil uygulama bu klasörü okur
      ├── centers.json            ← Tüm merkez listesi
      ├── analyses/
      │   ├── aku_tuam.json       ← AKÜ TUAM fiyat listesi
      │   ├── esogu_arum.json     ← ESOGÜ ARUM fiyat listesi
      │   └── dpu_iltem.json      ← DPÜ İLTEM fiyat listesi
      └── history/
          ├── aku_tuam.json       ← AKÜ TUAM fiyat geçmişi
          ├── esogu_arum.json
          └── dpu_iltem.json
```

---

## Tracker Nasıl Çalışır?

```
GitHub Actions tetikleyicisi (Aralık–Şubat ve Mayıs–Temmuz, her Pazartesi)
    ↓
tracker.py çalışır
    ↓
Her aktif merkez için:
    ├── Doğru fetcher'ı çağırır (html_table, html_then_pdf, manual)
    ├── Kayıtlı snapshot ile karşılaştırır
    ├── Değişiklik varsa → GitHub Issue açar + bildirim gönderir
    ├── Snapshot'ı günceller
    └── data/ klasörüne yapılandırılmış JSON yazar
    ↓
git commit + push (snapshot ve data değişiklikleri)
```

---

## Kurulum ve Çalıştırma

```bash
git clone https://github.com/YOUR_USERNAME/analizbul-data
cd analizbul-data
pip install -r requirements.txt

# Tüm aktif merkezleri çalıştır
python tracker.py

# Tek merkez
python tracker.py aku_tuam
```

---

## Yeni Merkez Eklemek

`config/centers.json` dosyasına yeni bir giriş ekleyin:

```json
{
  "id":          "yeni_merkez",
  "name":        "Üniversite MERKEZ",
  "university":  "Üniversite Adı",
  "city":        "Şehir",
  "url":         "https://merkez.edu.tr",
  "pricing_url": "https://merkez.edu.tr/fiyatlar/",
  "fetch_method": "html_table",
  "selector":    ".content table",
  "check_months": [12, 1, 2, 5, 6, 7],
  "active":      true
}
```

Desteklenen `fetch_method` değerleri: `html_table`, `html_then_pdf`, `manual`

---

## Pilot Merkezler

| ID | Merkez | Yöntem | Durum |
|---|---|---|---|
| `aku_tuam` | AKÜ TUAM ⭐ | `html_then_pdf` | ✅ Aktif |
| `esogu_arum` | ESOGÜ ARUM | `html_table` | ✅ Aktif |
| `dpu_iltem` | DPÜ İLTEM | `html_table` | ✅ Aktif |

---

## Lisans

Veriler CC0 (kamu malı) lisansı altında sunulmaktadır.
Tracker kodu MIT lisansı altındadır.
