from flask import Flask, request, jsonify
import datetime
import json
import os
from dotenv import load_dotenv
import psycopg2
import structlog
import logging

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

with open("log_file.json", "wt", encoding="utf-8") as log_fl:
    structlog.configure(
    processors=[structlog.processors.TimeStamper(fmt="iso"),
    structlog.processors.JSONRenderer()],
    logger_factory=structlog.WriteLoggerFactory(file=log_fl))
app = Flask(__name__)
# api = Api(app);



@app.route('/email', methods=['POST'])#, methods=['POST']
def post():
    conn = get_db_connection()
    cur = conn.cursor()
    data = request.data.decode('utf-8')
    print(data)
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
    cur.execute('INSERT INTO emails (received_timestamp, email_object) VALUES (%s, %s);', (timestamp, json_email_object))
    conn.commit()
    cur.close()
    return jsonify({'status': 200})

# GET /mailbox/email/<email_id:int>
# Returns a JSON object with the key "email" and an associated value of a String containing the entire email text
@app.route('/mailbox/email/<int:email_id>')
def get_email(email_id):
    with open("log_file.json", "wt", encoding="utf-8") as log_fl:
        structlog.configure(
        processors=[structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()],
        logger_factory=structlog.WriteLoggerFactory(file=log_fl))
        logger = structlog.get_logger()
        logger.info(event="email::id::folder::put", email_id=email_id)
    # structlog.stdlib.recreate_defaults(log_level=None)
    # structlog.dev.ConsoleRenderer(colors=False)
    # logging.basicConfig(format="%(message)s", filename='log_file.json', encoding='utf-8', level=logging.INFO)
    #logger.info(event="email::id::put", email_id=email_id)
    return jsonify({'status': 200})

# GET /mailbox/email/<email_id:int>/folder
# Get the folder containing the given email.  Examples of folders include "Inbox", "Archive", "Trash", and "Sent".
@app.route('/mailbox/email/')
def get_folder():
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)

# GET /mailbox/email/<email_id:int>/labels
# Returns a JSON object with the fields "email_id" and "labels".  The value for labels is a list of strings.  Valid labels include "spam", "read", and "important".  No label may be repeated.
@app.route('/mailbox/email/')
def get_json():
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)


# GET /mailbox/folder/<folder:str>
# Lists the emails in a given folder.  Returns a list of email_ids.
@app.route('/mailbox/email/')
def get_emails():
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)

# GET /mailbox/labels/<label:str>
# List emails with the given label.  Returns a list of email_ids.
@app.route('/mailbox/email/')
def get_emails_with_label():
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)

# PUT /mailbox/email/<email_id:int>/folder/<folder:str>
# Moves email to the given folder.  Folders include "Inbox", "Archive", "Trash", and "Sent".
@app.route('/mailbox/email/')
def put_email():
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)

# PUT /mailbox/email/<email_id:int>/label/<label:str>
# Mark the given email with the given label. Valid labels include "spam", "read", and "important".
@app.route('/mailbox/email/')
def put_email_label():
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)

# DELETE /mailbox/email/<email_id:int>/label/<label:str>
# Remove the given label from the given email. Valid labels include "spam", "read", and "important".
@app.route('/mailbox/email/')
def delete_email_label():
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)


if __name__ == '__main__':
    app.run(debug=True, port=8888)
    with open("log_file.json", "wt", encoding="utf-8") as log_fl:
        structlog.configure(
        processors=[structlog.processors.TimeStamp(fmt="iso"),
        structlog.processors.JSONRenderer()],
        logger_factory=structlog.WriteLoggerFactory(file=log_fl))