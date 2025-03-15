import os
import psycopg2
import fitz  # PyMuPDF
from pdf2image import convert_from_bytes
import re
from multiprocessing import Pool, cpu_count
import pytesseract

pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# PostgreSQL-Datenbankverbindung
DB_HOST = "192.168.178.121"
DB_NAME = "s3_backend_db"
DB_USER = "postgres"
DB_PASSWORD = "PostgresPassword"
DB_PORT = "5432"


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
        return None


def extract_text_from_pdf(pdf_data):
    try:
        doc = fitz.open(stream=pdf_data, filetype="pdf")
        extracted_text = ""
        page_count = len(doc)

        for page in doc:
            text = page.get_text("text")  # Versuch, Text direkt zu extrahieren
            if text.strip():
                extracted_text += text + "\n"
            else:
                images = convert_from_bytes(pdf_data, dpi=150)
                for img in images:
                    text = pytesseract.image_to_string(img, lang="deu",
                                                       config="--psm 3 --oem 1 -c preserve_interword_spaces=1")
                    extracted_text += text + "\n"
        return extracted_text.strip(), page_count
    except Exception as e:
        print(f"Fehler beim Extrahieren des PDFs: {e}")
        return None, 0


def clean_ocr_text(text):
    text = text.replace("-\n", "  ")
    text = re.sub(r"(?<!\n)\n(?!\n)", " ", text)  # Ersetze einzelne Zeilenumbrüche durch Leerzeichen
    text = re.sub(r"\n{3,}", "\n\n", text)  # Reduziert mehr als zwei Zeilenumbrüche auf zwei
    text = re.sub(r"\s+", " ", text).strip()
    return text


def process_single_pdf(pdf_id):
    conn = get_db_connection()
    if not conn:
        print("Keine Verbindung zur Datenbank. Beende Verarbeitung.")
        return None

    cursor = conn.cursor()
    cursor.execute("SELECT pdf FROM guidelines WHERE id = %s", (pdf_id,))
    pdf_row = cursor.fetchone()
    cursor.close()
    conn.close()

    if pdf_row is None or pdf_row[0] is None:
        return None

    pdf_data = bytes(pdf_row[0])
    text, page_count = extract_text_from_pdf(pdf_data)
    if text:
        text = clean_ocr_text(text)
        return pdf_id, text, page_count
    return None


def process_pdfs():
    conn = get_db_connection()
    if not conn:
        print("Keine Verbindung zur Datenbank. Beende Verarbeitung.")
        return

    cursor = conn.cursor()

    # Überprüfen, ob die Spalte `extracted_text` existiert
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'guidelines'")
    columns = [col[0] for col in cursor.fetchall()]
    if "extracted_text" not in columns:
        cursor.execute("ALTER TABLE guidelines ADD COLUMN extracted_text TEXT")
        conn.commit()

    # Lade alle PDFs, die noch nicht verarbeitet wurden
    cursor.execute("SELECT id FROM guidelines WHERE pdf IS NOT NULL AND extracted_text IS NULL")
    rows = cursor.fetchall()
    pdf_ids = [row[0] for row in rows]

    cursor.close()
    conn.close()

    if not pdf_ids:
        print("Alle PDFs wurden bereits verarbeitet.")
        return

    print(f"Starte Verarbeitung von {len(pdf_ids)} PDFs.")

    # Begrenze die Anzahl der gleichzeitig laufenden Prozesse
    num_workers = min(cpu_count(), 32)

    total_pages = 0
    processed_pdfs = 0

    with Pool(num_workers) as pool:
        results = pool.map(process_single_pdf, pdf_ids)

    conn = get_db_connection()
    if not conn:
        print("Fehler beim erneuten Verbinden mit der Datenbank.")
        return

    cursor = conn.cursor()

    for result in results:
        if result:
            pdf_id, text, page_count = result
            cursor.execute("UPDATE guidelines SET extracted_text = %s WHERE id = %s", (text.replace("\x00", ""), pdf_id))
            conn.commit()
            total_pages += page_count
            processed_pdfs += 1
            print(f"Text für PDF {pdf_id} gespeichert ({page_count} Seiten).")

    cursor.close()
    conn.close()
    print(f"Alle PDFs wurden verarbeitet. Gesamt: {processed_pdfs} Dokumente mit {total_pages} Seiten.")


if __name__ == "__main__":
    process_pdfs()
