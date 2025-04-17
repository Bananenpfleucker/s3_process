import os
import time
import textwrap
import psycopg2
import tiktoken
from openai import OpenAI
from dotenv import load_dotenv


load_dotenv()
client = OpenAI()

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')

CHUNK_SIZE = 4000
CHUNK_OVERLAP = 200
MODEL = "gpt-3.5-turbo"
MAX_FINAL_TOKENS = 12000
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


def count_tokens(text, model="gpt-3.5-turbo"):
    encoding = tiktoken.encoding_for_model(model)
    return len(encoding.encode(text))

def count_message_tokens(messages, model="gpt-3.5-turbo"):
    encoding = tiktoken.encoding_for_model(model)
    tokens_per_message = 4
    total_tokens = 0
    for msg in messages:
        total_tokens += tokens_per_message
        for key, value in msg.items():
            total_tokens += len(encoding.encode(value))
    total_tokens += 2  # priming
    return total_tokens



def get_db_connection():
    try:
        return psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


def retry_chat_request(messages, max_retries=MAX_RETRIES):
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=messages,
            )
            return response.choices[0].message.content
        except Exception as e:
            print(f"OpenAI error (attempt {attempt+1}/{max_retries}): {e}")
            time.sleep(RETRY_DELAY)
    print("All OpenAI attempts failed.")
    return None


def summarize_chunk(text):
    conn = get_db_connection()
    if not conn:
        return

    print("db connected")
    cursor = conn.cursor()

    cursor.execute("SELECT p.promptid, p.prompt_text FROM prompts p ORDER BY promptid DESC LIMIT 1;")
    row = cursor.fetchone()

    if not row:
        print("No prompt found.")
        cursor.close()
        conn.close()
        return

    print(row)

    messages = [
        {"role": "system", "content": row[1]},
        {"role": "user", "content": text}
    ]

    cursor.close()
    conn.close()

    return retry_chat_request(messages)



def split_text(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    chunks = []
    start = 0
    text_length = len(text)
    while start < text_length:
        end = min(start + chunk_size, text_length)
        chunks.append(text[start:end])
        start = end - overlap if end - overlap > start else end
    return chunks

def is_too_large(text):
    # 1 Token sollten 4 Zeichen sein
    return len(text) > MAX_FINAL_TOKENS * 4


def recursive_summarization(full_text, depth=0):
    chunks = split_text(full_text)
    summaries = []

    for i, chunk in enumerate(chunks):
        summary = summarize_chunk(chunk)
        if summary:
            summaries.append(summary)

    if not summaries:
        return None

    if len(summaries) == 1:
        return summaries[0]

    joined = "\n\n".join(summaries)

    if is_too_large(joined):
        print(f"Zwischensumme mit {len(summaries)} Abschnitten erneut zusammenfassen...")
        if depth > 10:
            print("Maximale Rekursionstiefe erreicht – gebe Zwischenstand zurück.")
            return joined
        return recursive_summarization(joined, depth=depth + 1)

    tokens = count_tokens(summaries)
    print(f"Chunk enthält {tokens} Tokens")

    return summarize_chunk(joined)



def process_one_summary():
    conn = get_db_connection()
    if not conn:
        return
    print("db connected")

    cursor = conn.cursor()
    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'guidelines'")
    columns = [col[0] for col in cursor.fetchall()]
    if "compressed_text" not in columns:
        cursor.execute("ALTER TABLE guidelines ADD COLUMN compressed_text TEXT")
        conn.commit()

    cursor.execute(
        "SELECT id, extracted_text FROM guidelines WHERE extracted_text IS NOT NULL AND compressed_text IS NULL LIMIT 1"
    )
    row = cursor.fetchone()

    if not row:
        print("No new text to sumarize found.")
        cursor.close()
        conn.close()
        return

    pdf_id, full_text = row
    print(f"Processing PDF {pdf_id} with {len(full_text)} characters...")

    summary = recursive_summarization(full_text)

    if summary:
        cursor.execute("UPDATE guidelines SET compressed_text = %s WHERE id = %s", (summary, pdf_id))
        conn.commit()
        print(f"Summary for PDF {pdf_id} saved.")
    else:
        print(f"Summary for PDF {pdf_id} failed.")

    cursor.close()
    conn.close()
    print("Processing done.")


if __name__ == "__main__":
    #while True:
    for i in range(1, 10):
        process_one_summary()
