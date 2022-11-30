import json
from flask import Flask, request, jsonify

app = Flask(__name__)
@app.route('/email', methods=['POST'])
def email():
    record = json.loads(request.data)
