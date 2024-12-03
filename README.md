# FonParam Veri Toplayıcı

Bu proje, fintables.com API'sini kullanarak yatırım fonlarının verilerini toplar ve MySQL veritabanında saklar.

## Özellikler

- Fon yönetim şirketlerinin verilerini toplar
  - Şirket logolarını otomatik olarak indirir ve `public/logos` dizinine kaydeder
  - Logolar orijinal dosya adlarıyla saklanır
- Fonların getiri oranlarını kaydeder
- Fonların geçmiş değerlerini takip eder
- Türkçe karakter desteği ile MySQL veritabanında saklama

## Kurulum

1. Projeyi klonlayın:
```bash
git clone https://github.com/kemalersin/fonparam-scraper.git
cd fonparam-scraper
```

2. Gerekli Python paketlerini yükleyin:
```bash
pip install -r requirements.txt
```

3. `.env.example` dosyasını `.env` olarak kopyalayın ve bilgilerinizi girin:
```bash
cp .env.example .env
```

4. `.env` dosyasında aşağıdaki bilgileri güncelleyin:
   - Veritabanı bağlantı bilgileri (`DB_*` değişkenleri)
   - Logo dosyalarının kaydedileceği dizin (`PUBLIC_PATH`)

5. MySQL veritabanınızı oluşturun.

6. Scripti çalıştırın:
```bash
python scrap.py
```

## Veritabanı Yapısı

Proje üç ana tablo kullanır:
- `fund_management_companies`: Fon yönetim şirketleri bilgileri
  - `logo`: Şirket logosunun `/logos/` dizinindeki yolu
- `fund_yields`: Fonların getiri oranları
- `fund_historical_values`: Fonların geçmiş değerleri

## Dizin Yapısı

```
fonparam/
├── scrap.py           # Ana script
├── .env              # Ortam değişkenleri
├── public/           # Statik dosyalar
│   └── logos/       # İndirilen logolar
└── README.md         # Bu dosya
```

## Geliştirme

Projeye katkıda bulunmak için:
1. Fork edin
2. Feature branch oluşturun (`git checkout -b feature/yeniOzellik`)
3. Değişikliklerinizi commit edin (`git commit -am 'Yeni özellik: X'`)
4. Branch'inizi push edin (`git push origin feature/yeniOzellik`)
5. Pull Request oluşturun