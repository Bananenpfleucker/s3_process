import os
import requests
import psycopg2
import sys

BASE_URL = "https://register.awmf.org"
API_URL = "https://leitlinien-api.awmf.org/v1/search"
DOWNLOAD_DIR = "downloads"

DB_HOST = "192.168.178.121"
DB_NAME = "s3_backend_db"
DB_USER = "postgres"
DB_PASSWORD = "PostgresPassword"
DB_PORT = "5432"

API_KEY = "MkI5Y1VIOEJ0ZGpoelNBVXRNM1E6WVFld0pBUF9RLVdJa012UHVPTmRQUQ=="  # API-Key

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def get_db_connection():
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
        sys.exit(1)

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS guidelines (
                        id SERIAL PRIMARY KEY,
                        awmf_guideline_id TEXT NOT NULL,
                        detail_page_url TEXT,
                        pdf_url TEXT,
                        pdf BYTEA,
                        extracted_text TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )''')
    conn.commit()
    cursor.close()
    conn.close()

def save_to_db(title, url, pdf_url, pdf_content):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO guidelines (awmf_guideline_id, detail_page_url, pdf_url, pdf) VALUES (%s, %s, %s, %s)",
                   (title, url, pdf_url, psycopg2.Binary(pdf_content)))
    conn.commit()
    cursor.close()
    conn.close()

def fetch_guidelines():
    headers = {
        "Accept": "application/json",
        "Api-Key": API_KEY,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
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
            print(f"Antwort: {response.text}")
            break

        data = response.json()

        if "records" not in data or not data["records"]:
            print("Keine Daten gefunden! API-Antwort:")
            print(data)
            break

        for entry in data["records"]:
            title = entry.get("AWMFGuidelineID", "Unbekannt")
            url = entry.get("AWMFDetailPage", "")
            pdf_links = entry.get("links", [])
            pdf_url = next((link["media"] for link in pdf_links if link.get("type") == "longVersion"), "")
            pdf_url = f"https://register.awmf.org/assets/guidelines/{pdf_url}" if pdf_url else ""
            guidelines.append((title, url, pdf_url))

        print(f"Geladene Einträge bisher: {len(guidelines)}")
        offset += limit

    return guidelines

def download_pdf(pdf_url):
    if not pdf_url:
        return None
    response = requests.get(pdf_url)
    if response.status_code == 200:
        print(f"PDF erfolgreich abgerufen: {pdf_url}")
        return response.content
    else:
        print(f"Fehler beim Herunterladen: {pdf_url}")
        return None

def scrape_pdfs():
    print("Starte AWMF-Leitlinien-Scraping...")
    init_db()

    guidelines = fetch_guidelines()
    print(f"Gesamt gefundene Leitlinien: {len(guidelines)}")

    for title, detail_url, pdf_url in guidelines:
        print(f"Verarbeite: {title}")
        pdf_content = download_pdf(pdf_url)
        if pdf_content:
            save_to_db(title, detail_url, pdf_url, pdf_content)

    print("Scraping abgeschlossen.")

def main():
    scrape_pdfs()

if __name__ == "__main__":
    main()

