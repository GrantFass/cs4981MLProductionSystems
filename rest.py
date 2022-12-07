from flask import Flask, request
from flask_restx import Resource, Api
from dotenv import dotenv_values
from init_db import getCursor
import time
import json
app = Flask(__name__)
api = Api(app);

curr = getCursor();
@app.route('/email')
class Email():
    def post(self):
        user_to = request.form['to']
        user_from = request.form['from']
        user_subject = request.form['subject']
        user_body = request.form['body']
        timestamp = time.time()
        email_object = {
            "to": user_to,
            "from": user_from,
            "subject": user_subject,
            "body": user_body
        }
        json_email_object = json.dumps(email_object)
        curr.execute('INSERT INTO emails (received_timestamp, email_object)'
        'VALUES (%f, %s)',
        (timestamp, json_email_object))
        curr.commit()
        return "Success";
curr.close();