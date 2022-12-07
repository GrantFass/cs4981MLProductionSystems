from flask import Flask, request
from flask_restx import Resource, Api

app = Flask(__name__)
api = Api(app);


@app.route('/email')
class Email():
    def post(self):
        user_to = request.form['to']
        user_from = request.form['from']
        user_subject = request.form['subject']
        user_body = request.form['body']
        return None;