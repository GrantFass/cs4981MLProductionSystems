import os
import psycopg2

conn = psycopg2.connect(
        host="localhost",
        database="email_ingestion",
        user=os.environ['DB_USERNAME'],
        password=os.environ['DB_PASSWORD'])

# Open a cursor to perform database operations
cur = conn.cursor()
def getCursor():
    return cur;