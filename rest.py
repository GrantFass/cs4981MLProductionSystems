from flask import Flask, request, jsonify
from flask_restx import Resource, Api
from dotenv import dotenv_values
from init_db import getCursor
import datetime
import json
import os
from dotenv import load_dotenv
import psycopg2
import base64

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



@app.route('/email', methods=['POST'])#, methods=['POST']
def post():
    conn = get_db_connection()
    cur = conn.cursor()
    data = request.data.decode('utf-8')
    print(data)
    # j = json.JSONDecoder(data)
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

# @app.route('/email')#, methods=['POST']
# class Email(Resource):
#     def get(self):
#         return 200
    
#     def put(self, email):
#         return email
    
#     def post(self, email):
#         # conn = get_db_connection()
#         # cur = conn.cursor()
#         # user_to = request.form['to']
#         # user_from = request.form['from']
#         # user_subject = request.form['subject']
#         # user_body = request.form['body']
#         # timestamp = datetime.datetime.now()
#         # email_object = {
#         #     "to": user_to,
#         #     "from": user_from,
#         #     "subject": user_subject,
#         #     "body": user_body
#         # }
#         # json_email_object = json.dumps(email_object)
#         # cur.execute('INSERT INTO emails (received_timestamp, email_object) VALUES (%f, %s);', (timestamp, json_email_object))
#         # cur.commit()
#         # cur.close()
#         return {'done', 200}
    

if __name__ == '__main__':
    app.run(debug=True)
    ## Use the below command to run the file
    # python rest.py
    
    ## Use the below command to test this file while it is running
    # curl -X POST http://localhost:5000/email -H 'Content-Type: application/json' -d '{"to": "someone@somewhere.com", "from": "someone_else@somewhere_else.com", "subject": "some important summary", "body": "Lots of text. More text."}'
    
