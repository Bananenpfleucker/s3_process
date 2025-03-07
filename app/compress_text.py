import psycopg2
import ollama
import textwrap

# PostgreSQL-Verbindungsdaten
DB_HOST = "192.168.178.121"
DB_NAME = "s3_backend_db"
DB_USER = "postgres"
DB_PASSWORD = "PostgresPassword"
DB_PORT = "5432"

# Maximale Zeichen pro Chunk für das LLM (damit die Anfragen handhabbar bleiben)
CHUNK_SIZE = 5000


def get_db_connection():
    """Stellt eine Verbindung zur PostgreSQL-Datenbank her."""
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


def summarize_text(text):
    """Sendet einen Textblock an Mistral für eine prägnante Zusammenfassung."""
    try:
        response = ollama.chat(
            model="mistral",
            messages=[
                {"role": "system",
                 "content": "Fasse den folgenden medizinischen Text auf Deutsch zusammen, ohne wichtige Details zu verlieren."},
                {"role": "user", "content": text}
            ]
        )
        return response["message"]["content"]
    except Exception as e:
        print(f"Fehler beim Abrufen der Zusammenfassung: {e}")
        return None


def summarize_final_text(text):
    try:
        response = ollama.chat(
            model="mistral",
            messages=[
                {"role": "system",
                 "content": ""},
                {"role": "user", "content": text}
            ]
        )
        return response["message"]["content"]
    except Exception as e:
        print(f"Fehler bei der finalen Zusammenfassung: {e}")
        return None


def recursive_summarization(full_text):
    """Zerlegt lange Texte in Blöcke, fasst sie zusammen und erstellt eine End-Zusammenfassung."""
    text_chunks = textwrap.wrap(full_text, CHUNK_SIZE)
    print(f"Zerlege Text in {len(text_chunks)} Abschnitte à {CHUNK_SIZE} Zeichen.")

    # 1⃣ Erste Runde der Zusammenfassung für jeden Chunk
    summaries = []
    for i, chunk in enumerate(text_chunks, 1):
        print(f"Zusammenfassung für Abschnitt {i}/{len(text_chunks)}...")
        summary = summarize_text(chunk)
        if summary:
            summaries.append(summary)

    # Falls es mehrere Abschnitte gibt, erstellen wir eine endgültige Zusammenfassung
    if len(summaries) > 1:
        final_text = "\n\n".join(summaries)
        print("Erstelle eine abschließende Zusammenfassung...")
        return summarize_final_text(final_text)

    return summaries[0] if summaries else None


def process_one_summary():
    """Holt einen einzelnen Text aus der DB, fasst ihn zusammen und speichert ihn."""
    conn = get_db_connection()
    if not conn:
        print("Fehler: Keine Verbindung zur Datenbank.")
        return

    cursor = conn.cursor()

    # Prüfen, ob die Spalte `compressed_text` existiert
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'guidelines'")
    columns = [col[0] for col in cursor.fetchall()]
    if "compressed_text" not in columns:
        cursor.execute("ALTER TABLE guidelines ADD COLUMN compressed_text TEXT")
        conn.commit()
        print("Spalte 'compressed_text' zur Tabelle 'guidelines' hinzugefügt.")

    # Einen einzelnen Datensatz holen, der noch nicht zusammengefasst wurde
    cursor.execute(
        "SELECT id, extracted_text FROM guidelines WHERE extracted_text IS NOT NULL AND compressed_text IS NULL LIMIT 1")
    row = cursor.fetchone()

    if not row:
        print("Keine neuen Texte zum Zusammenfassen gefunden.")
        cursor.close()
        conn.close()
        exit(0)
        return

    pdf_id, full_text = row
    print(f"Verarbeite PDF {pdf_id} mit {len(full_text)} Zeichen...")

    summary = recursive_summarization(full_text)

    if summary:
        cursor.execute("UPDATE guidelines SET compressed_text = %s WHERE id = %s", (summary, pdf_id))
        conn.commit()
        print(f"Zusammenfassung für PDF {pdf_id} gespeichert.")
    else:
        print(f"Fehler bei der Zusammenfassung für PDF {pdf_id}")

    cursor.close()
    conn.close()
    print("Verarbeitung abgeschlossen.")


if __name__ == "__main__":
    while True:
        process_one_summary()
