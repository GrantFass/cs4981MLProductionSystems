from flask import Flask, request, jsonify
import datetime
import json
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def get_db_connection():
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
    return conn


app = Flask(__name__)
# api = Api(app);


conn = get_db_connection()

@app.route('/email', methods=['POST'])#, methods=['POST']
def post():
    global conn
    cur = conn.cursor()
    data = request.data.decode('utf-8')
    # print(data)
    data = json.loads(data)
    timestamp = datetime.datetime.now()
    user_to = data['to']
    user_from = data['from']
    user_subject = data['subject']
    user_body = data['body']
    email_object = {
        "to": user_to,
        "from": user_from,
        "subject": user_subject,
        "body": user_body
    }
    json_email_object = json.dumps(email_object)
    cur.execute('INSERT INTO emails (received_timestamp, email_object) VALUES (%s, %s) RETURNING email_id;', (timestamp, json_email_object))
    row_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    return jsonify({'email_id': row_id})


if __name__ == '__main__':
    app.run(debug=True, port=8844)
