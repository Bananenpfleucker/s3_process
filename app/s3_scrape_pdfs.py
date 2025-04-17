import os
import requests
import psycopg2
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import time

from dotenv import load_dotenv
from pathlib import Path

# Lade Umgebungsvariablen aus einer .env-Datei
dotenv_path = Path('keys.env')
load_dotenv(dotenv_path=dotenv_path)

# PostgreSQL-Datenbankverbindung
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')

BASE_URL = "https://register.awmf.org"
API_URL = "https://leitlinien-api.awmf.org/v1/search"
API_KEY = "MkI5Y1VIOEJ0ZGpoelNBVXRNM1E6WVFld0pBUF9RLVdJa012UHVPTmRQUQ=="


def get_driver():
    """Erstellt eine optimierte Chrome WebDriver-Instanz im Headless-Modus."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")
    chrome_options.add_argument("--window-size=1920x1080")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    return driver


def wait_for_element(driver, xpath, timeout=10):
    """Wartet auf ein Element auf der Seite."""
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.XPATH, xpath))
    )


def parse_date(date_str):
    """Parst Datumswerte in das Format 'YYYY-MM-DD'."""
    if not date_str:
        return None
    try:
        clean_date = ''.join(filter(lambda c: c.isdigit() or c == ".", date_str)).strip()
        if not clean_date:
            return None
        return datetime.strptime(clean_date, "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError:
        print(f"Warnung: Ungültiges Datum '{date_str}', wird ignoriert.")
        return None


def get_db_connection():
    """Erstellt eine Verbindung zur PostgreSQL-Datenbank."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Fehler bei der Verbindung zur Datenbank: {e}")
        return None


def init_db():
    """Initialisiert die Datenbank und legt die Tabellen an, falls sie nicht existieren."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Erstelle die Tabelle, falls sie nicht existiert
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS guidelines (
            id SERIAL PRIMARY KEY,
            awmf_guideline_id TEXT NOT NULL,
            title TEXT,
            detail_page_url TEXT,
            pdf_url TEXT,
            pdf BYTEA,
            extracted_text TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            compressed_text TEXT,
            lversion TEXT,
            stand DATE,
            valid_until DATE,
            aktueller_hinweis TEXT
        )
    ''')

    # Sicherstellen, dass wichtige Indexe existieren
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_guideline_id ON guidelines (awmf_guideline_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_version ON guidelines (lversion);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_valid_until ON guidelines (valid_until);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON guidelines (created_at);")

    cursor.execute('''CREATE TABLE IF NOT EXISTS prompts (
                   promptid SERIAL PRIMARY KEY,
                   promttext TEXT NOT NULL);
                   ''')
    conn.commit()
    cursor.close()
    conn.close()
    print("Datenbank erfolgreich initialisiert.")


def fetch_guidelines():
    """Holt die Liste der verfügbaren Leitlinien von der API ab."""
    headers = {
        "Accept": "application/json",
        "Api-Key": API_KEY,
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://register.awmf.org/"
    }
    offset = 0
    limit = 50
    guidelines = []

    while True:
        params = {
            "doctype": "longVersion",
            "sorting": "relevance",
            "limit": limit,
            "offset": offset,
            "lang": "de"
        }
        response = requests.get(API_URL, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Fehler beim Abrufen der API: {response.status_code}")
            break

        data = response.json()
        if "records" not in data or not data["records"]:
            break

        for entry in data["records"]:
            guideline_id = entry.get("AWMFGuidelineID", "Unbekannt")
            url = entry.get("AWMFDetailPage", "")
            pdf_links = entry.get("links", [])
            pdf_url = next((link["media"] for link in pdf_links if link.get("type") == "longVersion"), "")
            pdf_url = f"{BASE_URL}/assets/guidelines/{pdf_url}" if pdf_url else ""
            guidelines.append((guideline_id, url, pdf_url))

        offset += limit
    return guidelines


def scrape_detail_page(detail_url):
    """Extrahiert die relevanten Metadaten von der Detailseite."""
    driver = get_driver()
    driver.get(detail_url)

    try:
        wait_for_element(driver, "//h1")
    except:
        print(f"Warnung: Keine 'Version' gefunden auf {detail_url}")

    def get_text(xpath):
        """Holt Text von einem bestimmten XPath oder gibt None zurück."""
        try:
            return driver.find_element(By.XPATH, xpath).text.strip()
        except:
            return None

    title = get_text("//h1")
    lversion = get_text("//ion-col[contains(text(),'Version:')]/following-sibling::ion-col")
    stand = parse_date(get_text("//ion-col[contains(text(),'Stand:')]/following-sibling::ion-col"))
    valid_until = parse_date(get_text("//ion-col[contains(text(),'Gültig bis:')]/following-sibling::ion-col"))
    aktueller_hinweis = get_text("//ion-col[contains(text(),'Aktueller Hinweis:')]/following-sibling::ion-col")

    driver.quit()
    return title, lversion, stand, valid_until, aktueller_hinweis


def download_pdf(pdf_url, retries=3):
    """Lädt das PDF herunter und gibt den Byte-Inhalt zurück."""
    if not pdf_url:
        return None

    for attempt in range(retries):
        response = requests.get(pdf_url, timeout=10)
        if response.status_code == 200:
            return response.content
        else:
            print(f"Fehler beim Herunterladen ({pdf_url}), Versuch {attempt + 1}/{retries}")
            time.sleep(1)

    print(f"Download fehlgeschlagen: {pdf_url}")
    return None


def save_to_db(guideline_id, title, detail_url, pdf_url, pdf_content, lversion, stand, valid_until, aktueller_hinweis):
    """Speichert die Daten in die Datenbank und überschreibt alte Versionen."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id, lversion FROM guidelines WHERE awmf_guideline_id = %s ORDER BY created_at DESC LIMIT 1",
                   (guideline_id,))
    result = cursor.fetchone()

    if result:
        db_id, db_version = result

        cursor.execute("UPDATE guidelines SET valid_until = %s WHERE id = %s", (valid_until, db_id))

        if db_version == lversion:
            # Wenn die Version identisch ist, nur den Titel aktualisieren
            cursor.execute("UPDATE guidelines SET title = %s WHERE id = %s", (title, db_id))
            conn.commit()
            print(f"{guideline_id} (Version: {lversion}) bereits in der DB – Titel aktualisiert.")
        else:
            # Version hat sich geändert – aktualisiere alle Daten
            cursor.execute("""
                UPDATE guidelines 
                SET title = %s, detail_page_url = %s, pdf_url = %s, pdf = %s, lversion = %s, stand = %s, 
                    valid_until = %s, aktueller_hinweis = %s, compressed_text = NULL
                WHERE id = %s
            """, (title, detail_url, pdf_url, psycopg2.Binary(pdf_content) if pdf_content else None,
                  lversion, stand, valid_until, aktueller_hinweis, db_id))
            conn.commit()
            print(f"{guideline_id} (Neue Version: {lversion}) – Daten aktualisiert.")
    else:
        # Neuer Eintrag
        cursor.execute("""
            INSERT INTO guidelines (
                awmf_guideline_id, title, detail_page_url, pdf_url, pdf,
                lversion, stand, valid_until, aktueller_hinweis, compressed_text
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
        """, (guideline_id, title, detail_url, pdf_url,
              psycopg2.Binary(pdf_content) if pdf_content else None,
              lversion, stand, valid_until, aktueller_hinweis))
        conn.commit()
        print(f"{guideline_id} (Neue Eintrag)")

    cursor.close()
    conn.close()


def scrape_pdfs():
    """Holt Daten von der API und verarbeitet sie."""
    init_db()
    guidelines = fetch_guidelines()

    for guideline_id, detail_url, pdf_url in guidelines:
        print(f"Verarbeite: {guideline_id}")
        title, lversion, stand, valid_until, aktueller_hinweis = scrape_detail_page(detail_url)
        pdf_content = download_pdf(pdf_url)
        save_to_db(guideline_id, title, detail_url, pdf_url, pdf_content, lversion, stand, valid_until,
                   aktueller_hinweis)


if __name__ == "__main__":
    scrape_pdfs()
