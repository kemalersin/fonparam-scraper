import cloudscraper
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import requests
from pathlib import Path

# .env dosyasını yükle
load_dotenv()

# Veritabanı bağlantı bilgileri
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': os.getenv('DB_CHARSET'),
    'collation': os.getenv('DB_COLLATION')
}

# Public dizini yolu
PUBLIC_PATH = os.getenv('PUBLIC_PATH')

def download_and_save_logo(logo_url):
    if not logo_url:
        return None
        
    try:
        # Public dizininin varlığını kontrol et ve oluştur
        logo_dir = Path(PUBLIC_PATH) / 'logos'
        logo_dir.mkdir(parents=True, exist_ok=True)
        
        # URL'den orijinal dosya adını al
        original_filename = Path(logo_url).name
        file_path = logo_dir / original_filename
        
        # Eğer dosya zaten varsa, direkt dosya adını döndür
        if file_path.exists():
            print(f"Logo zaten mevcut: {original_filename}")
            return original_filename
            
        # Logo dosyasını indir
        response = requests.get(logo_url)
        response.raise_for_status()
        
        # Dosyayı kaydet
        with open(file_path, 'wb') as f:
            f.write(response.content)
            
        print(f"Logo başarıyla indirildi ve kaydedildi: {original_filename}")
        return original_filename
        
    except Exception as e:
        print(f"Logo indirme hatası ({logo_url}): {e}")
        return None

def create_tables_if_not_exists(connection):
    try:
        cursor = connection.cursor()
        
        # Fon yönetim şirketleri tablosu
        create_companies_table = """
        CREATE TABLE IF NOT EXISTS fund_management_companies (
            code VARCHAR(10) PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            logo VARCHAR(255)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_turkish_ci
        """
        
        # Fon getirileri tablosu
        create_yields_table = """
        CREATE TABLE IF NOT EXISTS fund_yields (
            code VARCHAR(10) PRIMARY KEY,
            management_company_id VARCHAR(10),
            title VARCHAR(255) NOT NULL,
            type VARCHAR(100),
            tefas BOOLEAN,
            yield_1m DECIMAL(10,4),
            yield_3m DECIMAL(10,4),
            yield_6m DECIMAL(10,4),
            yield_ytd DECIMAL(10,4),
            yield_1y DECIMAL(10,4),
            yield_3y DECIMAL(10,4),
            yield_5y DECIMAL(10,4)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_turkish_ci
        """
        
        # Fon geçmiş değerler tablosu
        create_historical_values_table = """
        CREATE TABLE IF NOT EXISTS fund_historical_values (
            code VARCHAR(10),
            date DATE,
            value DECIMAL(10,6),
            PRIMARY KEY (code, date)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_turkish_ci
        """
        
        cursor.execute(create_companies_table)
        cursor.execute(create_yields_table)
        cursor.execute(create_historical_values_table)
        connection.commit()
        
    except Error as e:
        print(f"Tablo oluşturma hatası: {e}")

def get_last_date_for_fund(connection, fund_code):
    try:
        cursor = connection.cursor()
        query = """
        SELECT MAX(date) FROM fund_historical_values
        WHERE code = %s
        """
        cursor.execute(query, (fund_code,))
        result = cursor.fetchone()[0]
        
        if result:
            # Eğer son tarih bugünse None döndür (güncelleme gerekmez)
            if result == datetime.now().date():
                return None
            # Son tarihten bir gün sonrasını döndür
            return result + timedelta(days=1)
        else:
            # 5 yıl öncesinin tarihini döndür
            return datetime.now().date() - timedelta(days=5*365)
            
    except Error as e:
        print(f"Son tarih sorgulama hatası: {e}")
        return datetime.now().date() - timedelta(days=5*365)

def insert_historical_values(connection, fund_code, historical_data):
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO fund_historical_values (code, date, value)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
        value = VALUES(value)
        """
        
        values = []
        for item in historical_data['results']['data']:
            values.append((
                fund_code,
                item['date'],
                item[fund_code]
            ))
        
        cursor.executemany(insert_query, values)
        connection.commit()
        print(f"{cursor.rowcount} geçmiş değer kaydı başarıyla eklendi/güncellendi - {fund_code}")
    except Error as e:
        print(f"Geçmiş değer verisi ekleme hatası ({fund_code}): {e}")

def insert_companies(connection, companies):
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO fund_management_companies (code, title, logo)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
        title = VALUES(title),
        logo = VALUES(logo)
        """
        for company in companies:
            # Logo'yu indir ve kaydet, sadece dosya adını al
            logo_filename = download_and_save_logo(company['logo'])
            
            values = (company['code'], company['title'], logo_filename)
            cursor.execute(insert_query, values)
        
        connection.commit()
        print(f"{cursor.rowcount} şirket kaydı başarıyla eklendi/güncellendi.")
    except Error as e:
        print(f"Şirket verisi ekleme hatası: {e}")

def insert_yields(connection, yields_data):
    try:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO fund_yields (
            code, management_company_id, title, type, tefas,
            yield_1m, yield_3m, yield_6m, yield_ytd, yield_1y, yield_3y, yield_5y
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        management_company_id = VALUES(management_company_id),
        title = VALUES(title),
        type = VALUES(type),
        tefas = VALUES(tefas),
        yield_1m = VALUES(yield_1m),
        yield_3m = VALUES(yield_3m),
        yield_6m = VALUES(yield_6m),
        yield_ytd = VALUES(yield_ytd),
        yield_1y = VALUES(yield_1y),
        yield_3y = VALUES(yield_3y),
        yield_5y = VALUES(yield_5y)
        """
        
        for fund in yields_data['results']:
            values = (
                fund['code'],
                fund['management_company_id'],
                fund['title'],
                fund['type'],
                fund['tefas'],
                fund.get('yield_1m'),
                fund.get('yield_3m'),
                fund.get('yield_6m'),
                fund.get('yield_ytd'),
                fund.get('yield_1y'),
                fund.get('yield_3y'),
                fund.get('yield_5y')
            )
            cursor.execute(insert_query, values)
        
        connection.commit()
        print(f"{cursor.rowcount} fon getirisi kaydı başarıyla eklendi/güncellendi.")
        return yields_data['results']  # Fonların listesini döndür
    except Error as e:
        print(f"Fon getirisi verisi ekleme hatası: {e}")
        return []

try:
    # Veritabanına bağlan
    connection = mysql.connector.connect(**db_config)
    
    if connection.is_connected():
        print("MySQL veritabanına başarıyla bağlandı")
        
        # Tabloları oluştur
        create_tables_if_not_exists(connection)
        
        # Scraper oluştur
        scraper = cloudscraper.create_scraper()
        
        # Şirket verilerini çek ve kaydet
        companies = scraper.get("https://api.fintables.com/fund-management-companies/").json()
        insert_companies(connection, companies)
        
        # Fon getirilerini çek ve kaydet
        yields_data = scraper.get("https://api.fintables.com/funds/yield/").json()
        funds = insert_yields(connection, yields_data)
        
        # Her bir fon için geçmiş değerleri çek ve kaydet
        for fund in funds:
            fund_code = fund['code']
            start_date = get_last_date_for_fund(connection, fund_code)
            
            # Eğer start_date None ise, bu fon için bugünün verisi zaten var demektir
            if start_date is None:
                print(f"Atlanan fon (bugünün verisi mevcut): {fund_code}")
                continue
            
            # Geçmiş değerleri çek
            historical_url = f"https://api.fintables.com/funds/{fund_code}/chart/?start_date={start_date.strftime('%Y-%m-%d')}"
            print(f"Geçmiş değerler çekiliyor: {fund_code} - {start_date}")
            
            try:
                historical_data = scraper.get(historical_url).json()
                insert_historical_values(connection, fund_code, historical_data)
            except Exception as e:
                print(f"Geçmiş değer çekme hatası ({fund_code}): {e}")
                continue

except Error as e:
    print(f"MySQL bağlantı hatası: {e}")

finally:
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("MySQL bağlantısı kapatıldı")