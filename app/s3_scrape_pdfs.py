import os
import requests
import sqlite3

BASE_URL = "https://register.awmf.org"
API_URL = "https://leitlinien-api.awmf.org/v1/search"
DOWNLOAD_DIR = "downloads"
DB_FILE = "leitlinien.db"
API_KEY = "MkI5Y1VIOEJ0ZGpoelNBVXRNM1E6WVFld0pBUF9RLVdJa012UHVPTmRQUQ=="  # Gefundener API-Key

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS guidelines (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT,
                        url TEXT,
                        pdf_url TEXT,
                        pdf BLOB)''')
    conn.commit()
    conn.close()

    cursor = conn.cursor()
    cursor.execute("INSERT INTO guidelines (title, url, pdf_url, pdf) VALUES (?, ?, ?, ?)",
                   (title, url, pdf_url, pdf_content))
    conn.commit()
    conn.close()

def fetch_guidelines():
    headers = {
        "Accept": "application/json",
        "Api-Key": API_KEY,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Referer": "https://register.awmf.org/"
    }
    offset = 0
    limit = 50  # Anzahl pro Anfrage
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
            title = entry.get("AWMFGuidelineID", "Unbekannt")  # Anpassung für korrekten Titel
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
def scrapePDFs():
    print("Starte AWMF-Leitlinien-Scraping...")
    init_db()

    guidelines = fetch_guidelines()
    print(f"Gesamt gefundene Leitlinien: {len(guidelines)}")

    for title, detail_url, pdf_url in guidelines:
        print(f"Verarbeite: {title}")
        pdf_content = download_pdf(pdf_url)
        save_to_db(title, detail_url, pdf_url, pdf_content)

    print("Scraping abgeschlossen.")

def main():
    scrapePDFs()




if __name__ == "__main__":
    main()
