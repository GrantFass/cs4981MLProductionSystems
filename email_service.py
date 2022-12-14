from flask import Flask, request, jsonify
import datetime
import json
import os
from dotenv import load_dotenv
import psycopg2
import structlog

load_dotenv()

idx = 0

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


conn = get_db_connection()

@app.route('/email', methods=['POST'])#, methods=['POST']
def post():
    global idx, conn
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
    cur.execute('INSERT INTO emails (received_timestamp, email_object) VALUES (%s, %s);', (timestamp, json_email_object))
    conn.commit()
    cur.close()
    idx += 1 # TODO: fixme
    return jsonify({'email_id': idx})

# GET /mailbox/email/<email_id:int>
# Returns a JSON object with the key "email" and an associated value of a String containing the entire email text
@app.route('/mailbox/email/<int:email_id>',  methods=['GET'])
def get_email(email_id):
    logger = structlog.get_logger()
    logger.info(event="email::id::get", email_id=email_id)
    #return jsonify({'email': data})
    return jsonify({'status': 200})

# GET /mailbox/email/<email_id:int>/folder
# Get the folder containing the given email.  Examples of folders include "Inbox", "Archive", "Trash", and "Sent".
@app.route('/mailbox/email/<int:email_id>/<string:folder>', methods=['GET'])
def get_folder(email_id, folder):
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::get", email_id=email_id, folder=folder)
    #return jsonify({'folder': data})
    return jsonify({'status': 200})


# GET /mailbox/email/<email_id:int>/labels
# Returns a JSON object with the fields "email_id" and "labels".  The value for labels is a list of strings.  Valid labels include "spam", "read", and "important".  No label may be repeated.
@app.route('/mailbox/email/<int:email_id>/labels',  methods=['GET'])
def get_json(email_id):
    logger = structlog.get_logger()
    logger.info(event="email::id::labels::get", email_id=email_id)
    return jsonify({'status': 200})


# GET /mailbox/folder/<folder:str>
# Lists the emails in a given folder.  Returns a list of email_ids.
@app.route('/mailbox/folder/<string:folder>',  methods=['GET'])
def get_emails(folder):
    logger = structlog.get_logger()
    logger.info(event="email::folder::get", folder=folder)
    return jsonify({'status': 200})

# GET /mailbox/labels/<label:str>
# List emails with the given label.  Returns a list of email_ids.
@app.route('/mailbox/labels/<string:label>',  methods=['GET'])
def get_emails_with_label(label):
    logger = structlog.get_logger()
    logger.info(event="email::label::get", label=label)
    return jsonify({'status': 200})

# PUT /mailbox/email/<email_id:int>/folder/<folder:str>
# Moves email to the given folder.  Folders include "Inbox", "Archive", "Trash", and "Sent".
@app.route('/mailbox/email/<int:email_id>/folder/<string:folder>',  methods=['PUT'])
def put_email(email_id, folder):
    logger = structlog.get_logger()
    logger.info(event="email::id::folder::put", email_id=email_id, folder=folder)
    return jsonify({'status': 200})

# PUT /mailbox/email/<email_id:int>/label/<label:str>
# Mark the given email with the given label. Valid labels include "spam", "read", and "important".
@app.route('/mailbox/email/<int:email_id>/label/<string:label>',  methods=['PUT'])
def put_email_label(email_id, label):
    logger = structlog.get_logger()
    logger.info(event="email::id::label::put", email_id=email_id, label=label)
    return jsonify({'status': 200})

# DELETE /mailbox/email/<email_id:int>/label/<label:str>
# Remove the given label from the given email. Valid labels include "spam", "read", and "important".
@app.route('/mailbox/email/<int:email_id>/label/<string:label>',  methods=['DELETE'])
def delete_email_label(email_id, label):
    logger = structlog.get_logger()
    logger.info(event="email::id::label::delete", email_id=email_id, label=label)
    return jsonify({'status': 200})


if __name__ == '__main__':
    with open("log_file.json", "wt", encoding="utf-8") as log_fl:
        structlog.configure(
        processors=[structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()],
        logger_factory=structlog.WriteLoggerFactory(file=log_fl))
        app.run(debug=True, port=8888)
