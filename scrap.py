from datetime import datetime, timedelta
import cloudscraper
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os
import requests
from pathlib import Path

class FonParamDB:
    def __init__(self):
        load_dotenv()
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME'),
            'charset': os.getenv('DB_CHARSET'),
            'collation': os.getenv('DB_COLLATION')
        }
        self.connection = None
        
    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            print("MySQL veritabanına başarıyla bağlandı")
            self.create_tables_if_not_exists()
        except Error as e:
            print(f"MySQL bağlantı hatası: {e}")
            raise
            
    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("MySQL bağlantısı kapatıldı")
            
    def create_tables_if_not_exists(self):
        try:
            cursor = self.connection.cursor()
            
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
            self.connection.commit()
            
        except Error as e:
            print(f"Tablo oluşturma hatası: {e}")

class LogoManager:
    def __init__(self):
        self.public_path = os.getenv('PUBLIC_PATH')
        
    def download_and_save(self, logo_url):
        if not logo_url:
            return None
            
        try:
            logo_dir = Path(self.public_path) / 'logos'
            logo_dir.mkdir(parents=True, exist_ok=True)
            
            original_filename = Path(logo_url).name
            file_path = logo_dir / original_filename
            
            if file_path.exists():
                print(f"Logo zaten mevcut: {original_filename}")
                return original_filename
                
            response = requests.get(logo_url)
            response.raise_for_status()
            
            with open(file_path, 'wb') as f:
                f.write(response.content)
                
            print(f"Logo başarıyla indirildi ve kaydedildi: {original_filename}")
            return original_filename
            
        except Exception as e:
            print(f"Logo indirme hatası ({logo_url}): {e}")
            return None

class FintablesAPI:
    def __init__(self):
        self.scraper = cloudscraper.create_scraper()
        self.base_url = "https://api.fintables.com"
        
    def get_companies(self):
        try:
            response = self.scraper.get(f"{self.base_url}/fund-management-companies/")
            companies = response.json()
            
            if not isinstance(companies, list):
                raise ValueError("Şirket verileri beklenen formatta değil (liste bekleniyor)")
                
            return companies
        except Exception as e:
            print(f"Şirket verilerini çekerken hata oluştu: {e}")
            raise
            
    def get_yields(self):
        try:
            response = self.scraper.get(f"{self.base_url}/funds/yield/")
            yields_data = response.json()
            
            if not isinstance(yields_data, dict) or 'results' not in yields_data:
                raise ValueError("Fon getirileri beklenen formatta değil")
                
            return yields_data
        except Exception as e:
            print(f"Fon getirilerini çekerken hata oluştu: {e}")
            raise
            
    def get_historical_values(self, fund_code, start_date):
        try:
            url = f"{self.base_url}/funds/{fund_code}/chart/?start_date={start_date.strftime('%Y-%m-%d')}"
            response = self.scraper.get(url)
            data = response.json()
            
            if not isinstance(data, dict) or 'results' not in data or 'data' not in data['results']:
                raise ValueError("Geçmiş değerler beklenen formatta değil")
                
            return data
        except Exception as e:
            print(f"Geçmiş değer çekme hatası ({fund_code}): {e}")
            raise

class FundDataManager:
    def __init__(self):
        self.db = FonParamDB()
        self.api = FintablesAPI()
        self.logo_manager = LogoManager()
        
    def run(self):
        try:
            self.db.connect()
            
            # Şirket verilerini çek ve kaydet
            companies = self.api.get_companies()
            self.insert_companies(companies)
            
            # Fon getirilerini çek ve kaydet
            yields_data = self.api.get_yields()
            funds = self.insert_yields(yields_data)
            
            # Geçmiş değerleri çek ve kaydet
            self.process_historical_values(funds)
            
        finally:
            self.db.close()
            
    def insert_companies(self, companies):
        try:
            cursor = self.db.connection.cursor()
            insert_query = """
            INSERT INTO fund_management_companies (code, title, logo)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            logo = VALUES(logo)
            """
            for company in companies:
                logo_filename = self.logo_manager.download_and_save(company['logo'])
                values = (company['code'], company['title'], logo_filename)
                cursor.execute(insert_query, values)
            
            self.db.connection.commit()
            print(f"{cursor.rowcount} şirket kaydı başarıyla eklendi/güncellendi.")
        except Error as e:
            print(f"Şirket verisi ekleme hatası: {e}")
            
    def insert_yields(self, yields_data):
        try:
            cursor = self.db.connection.cursor()
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
            
            self.db.connection.commit()
            print(f"{cursor.rowcount} fon getirisi kaydı başarıyla eklendi/güncellendi.")
            return yields_data['results']
        except Error as e:
            print(f"Fon getirisi verisi ekleme hatası: {e}")
            return []
            
    def process_historical_values(self, funds):
        for fund in funds:
            fund_code = fund['code']
            start_date = self.get_last_date_for_fund(fund_code)
            
            if start_date is None:
                print(f"Atlanan fon (bugünün verisi mevcut): {fund_code}")
                continue
                
            try:
                historical_data = self.api.get_historical_values(fund_code, start_date)
                self.insert_historical_values(fund_code, historical_data)
            except Exception:
                continue
                
    def get_last_date_for_fund(self, fund_code):
        try:
            cursor = self.db.connection.cursor()
            query = """
            SELECT MAX(date) FROM fund_historical_values
            WHERE code = %s
            """
            cursor.execute(query, (fund_code,))
            result = cursor.fetchone()[0]
            
            if result:
                if result == datetime.now().date():
                    return None
                return result + timedelta(days=1)
            else:
                return datetime.now().date() - timedelta(days=5*365)
                
        except Error as e:
            print(f"Son tarih sorgulama hatası: {e}")
            return datetime.now().date() - timedelta(days=5*365)
            
    def insert_historical_values(self, fund_code, historical_data):
        try:
            cursor = self.db.connection.cursor()
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
            self.db.connection.commit()
            print(f"{cursor.rowcount} geçmiş değer kaydı başarıyla eklendi/güncellendi - {fund_code}")
        except Error as e:
            print(f"Geçmiş değer verisi ekleme hatası ({fund_code}): {e}")

if __name__ == "__main__":
    manager = FundDataManager()
    manager.run()