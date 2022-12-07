import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

host=os.getenv('POSTGRES_HOST')
database=os.getenv('POSTGRES_DATABASE')
user=os.getenv('POSTGRES_USERNAME')
password=os.getenv('POSTGRES_PASSWORD')
port = os.getenv('POSTGRES_PORT')

# TODO: crash if null

conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port = port)

# Open a cursor to perform database operations
cur = conn.cursor()
def getCursor():
    return cur;