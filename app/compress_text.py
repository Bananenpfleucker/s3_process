import psycopg2
import ollama
import textwrap
from multiprocessing import Pool, cpu_count

# PostgreSQL-Datenbankverbindung
DB_HOST = "192.168.178.121"
DB_NAME = "s3_backend_db"
DB_USER = "postgres"
DB_PASSWORD = "PostgresPassword"
DB_PORT = "5432"

# Maximale Zeichenanzahl pro Chunk für die erste Zusammenfassung
CHUNK_SIZE = 4000  # Kleiner für effizientere Verarbeitung

# Modellwahl
SMALL_MODEL = "gemma:2b"  # Klein & schnell für erste Verdichtung
LARGE_MODEL = "gemma:2b"  # Hochwertige medizinische Zusammenfassung

# Maximale Anzahl paralleler Prozesse
NUM_PROCESSES = max(cpu_count() - 1, 1)  # Nutzt alle CPU-Kerne außer 1 für Performance


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


def summarize_with_small_model(text):
    """Erste Stufe: Verdichtet lange Texte mit einem schnellen Modell."""
    try:
        response = ollama.chat(
            model=SMALL_MODEL,
            messages=[
                {"role": "system",
                 "content": "Du bist ein medizinisches KI-Modell. Fasse den folgenden Text so zusammen, dass die Kernaussagen erhalten bleiben. Verzichte auf allgemeine Erklärungen."},
                {"role": "user", "content": text}
            ]
        )
        return response["message"]["content"]
    except Exception as e:
        print(f"Fehler bei der ersten Zusammenfassung: {e}")
        return None


def summarize_with_large_model(text):
    """Zweite Stufe: Hochwertige Zusammenfassung mit Meditron-7B."""
    try:
        response = ollama.chat(
            model=LARGE_MODEL,
            messages=[
                {"role": "system",
                 "content": "Fasse den folgenden medizinischen Fachtext präzise zusammen. Erhalte alle relevanten Details, aber reduziere unnötige Wiederholungen."},
                {"role": "user", "content": text}
            ]
        )
        return response["message"]["content"]
    except Exception as e:
        print(f"Fehler bei der finalen Zusammenfassung: {e}")
        return None


def recursive_summarization(full_text):
    """Zerteilt Text in Blöcke, fasst sie mit Gemma-2B zusammen und erstellt eine finale Meditron-7B-Zusammenfassung."""
    text_chunks = textwrap.wrap(full_text, CHUNK_SIZE)
    print(f"Zerlege Text in {len(text_chunks)} Abschnitte à {CHUNK_SIZE} Zeichen.")

    # Erste Stufe der Zusammenfassung mit Gemma-2B
    summaries = []
    with Pool(NUM_PROCESSES) as pool:
        summaries = pool.map(summarize_with_small_model, text_chunks)

    summaries = [s for s in summaries if s]  # Entferne None-Werte

    # Falls es mehrere Abschnitte gibt, nutzen wir Meditron-7B für die finale Zusammenfassung
    if len(summaries) > 1:
        final_text = "\n\n".join(summaries)
        print(f"Erstelle finale Zusammenfassung mit {LARGE_MODEL}...")
        return summarize_with_large_model(final_text)

    return summaries[0] if summaries else None


def process_one_summary():
    """Holt einen einzelnen Text aus der DB, fasst ihn zusammen und speichert ihn."""
    conn = get_db_connection()
    if not conn:
        print("Fehler: Keine Verbindung zur Datenbank.")
        return

    cursor = conn.cursor()

    # Prüfen, ob `compressed_text` existiert
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
