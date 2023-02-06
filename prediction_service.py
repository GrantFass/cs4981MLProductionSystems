# CS 4981 ML Production Systems Project 4
# File is used for Offline Model Development and Prediction Service

import datetime
import json
import os
import pickle
import boto3
from dotenv import load_dotenv
from datetime import datetime
from flask import Flask, request, jsonify
from botocore.errorfactory import ClientError
import structlog  # for event logging
import pandas as pd
import numpy as np
import os
import json
import glob
from sklearn.feature_extraction.text import CountVectorizer
import scipy
from scipy.sparse import csr_matrix
from sklearn.decomposition import TruncatedSVD
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
from sklearn.cluster import DBSCAN
from sklearn import svm
from sklearn import metrics
from sklearn.model_selection import StratifiedKFold
from imblearn.under_sampling import RandomUnderSampler
from collections import Counter
from sklearn.feature_extraction.text import TfidfVectorizer as vec

# create the flask app for the rest endpoints
app = Flask(__name__)

# load the environment files
load_dotenv()

# the model used to predict
model = []


# set up the structured logging file
with open(os.getenv('PREDICTION_LOG_PATH'), "wt", encoding="utf-8") as log_fl:
    structlog.configure(
        processors=[structlog.processors.TimeStamper(fmt="iso"),
                    structlog.processors.JSONRenderer()],
        logger_factory=structlog.WriteLoggerFactory(file=log_fl))

# Import from MinIO
# set up the connection to the S3 object store. Login properties are read from the environment file
# going to need to connect to two separate buckets as well. One bucket will be for storing files from the app
# the other bucket will be for storing the log files. It should only be pushed every 15? minutes.
# the timed push will be defined by a command line arg on launch. Needs a separate thread or program.
s3_resource = boto3.resource('s3',
                             endpoint_url=os.getenv('ENDPOINT_URL'),
                             aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                             aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                             aws_session_token=None,
                             config=boto3.session.Config(signature_version='s3v4'),
                             verify=False
                             )

s3_client = boto3.client('s3',
                             endpoint_url=os.getenv('ENDPOINT_URL'),
                             aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                             aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                             aws_session_token=None,
                             config=boto3.session.Config(signature_version='s3v4'),
                             verify=False
                             )


# going to need to add instructions on how to set up minio... Launch args: minio server minio_data
def send_to_bucket(body: str, log_name="", bucket_name="joined-out"):
    if not log_name:
        log_name = "log_file_%s.json" % datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    s3_resource.Bucket(bucket_name).put_object(Key=log_name, Body=body)


def read_from_s3(file_name: str, bucket_name="joined-out"):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html
    obj = s3_resource.Object(bucket_name=bucket_name, key=file_name)
    response = obj.get()
    data = response['Body'].read()
    # print(data)
    if isinstance(data, bytes):
        data = data.decode()
    elif not isinstance(data, str):
        data = str(data)  # https://stackoverflow.com/a/45928164 possible base64 check
    return data


def read_from_s3_pipelines(file_name: str, bucket_name="joined-out"):
    # https://stackoverflow.com/questions/36205481/read-file-content-from-s3-bucket-with-boto3
    bucket = s3_resource.Bucket(bucket_name)
    # Iterates through all the objects, doing the pagination for you. Each obj
    # is an ObjectSummary, so it doesn't contain the body. You'll need to call
    # get to get the whole body.
    pipelines = []
    for obj in bucket.objects.all():
        key = obj.key
        if file_name in str(key):
            temp = str(key).removeprefix('pipeline_')
            temp = temp.removesuffix('.pkl')
            time = datetime.strptime(temp, "%Y%m%d-%H%M%S")
            # print("file_name: %s\ttime = %s" % (str(key), time))
            pipelines.append((str(key), time))
            # data = obj.get()['Body'].read()
    if pipelines:
        pipelines = sorted(pipelines, key = lambda x: x[1]) # sort by second item
        # want to grab last item's filename
        filename = pipelines[-1][0]
        # https://python-bloggers.com/2022/07/persisting-data-with-pickle-s3/
        s3_client.download_file(bucket_name, filename, filename)
        try:
            with open(filename, "rb") as file:
                data = pickle.load(file)
        except (FileNotFoundError, EOFError):
            # You'll arrive here on the first iteration.
            data = None
    else:
        print("ERROR KEY NOT FOUND")
        data = None
    return data


def check_for_file_s3(file_name: str, bucket_name="joined-out"):
    # return s3_client.head_object(Bucket=bucket_name, Key=file_name)['ContentLength']
    try:
        return s3_resource.Object(bucket_name, file_name).content_length
    except ClientError:
        return -1
    
    
def run_flask():
    global model
    # Load the most recent pipeline that was trained
    model = read_from_s3_pipelines("pipeline")
    print(model)
    
    with open(os.getenv('PREDICTION_LOG_PATH'), "wt", encoding="utf-8") as log_fl:
        structlog.configure(
            processors=[structlog.processors.TimeStamper(fmt="iso"),
                        structlog.processors.JSONRenderer()],
            logger_factory=structlog.WriteLoggerFactory(file=log_fl))
        app.run(debug=True, port=8888)
       

@app.route('/classify', methods=['GET'])
def classify_email():
    """
    Takes a JSON object as payload.  
    The object contains the key “email” with the value being another object.  
    The email object should have the key “body” with the value being a String.  
    The request should return another JSON object with the key 
    “predicted_class” and a value of “spam” or “ham.”
    """
    global model
    
    # get the data from the request
    data = request.data.decode('utf-8')
    # print(data)
    data = json.loads(data)
    email = data['email'] # email object should have keys 'to', 'from', 'body', and 'subject'
    
    # print(email)
    sample = pd.DataFrame([email])
    if 'to_address' in sample.columns:
        sample['to'] = sample['to_address']
        sample = sample.drop(columns=['to_address'])
    if 'from_address' in sample.columns:
        sample['from'] = sample['from_address']
        sample = sample.drop(columns=['from_address'])
    if 'label' in sample.columns:
        sample = sample.drop(columns=['label'])
    
    # print(sample.info())
    # print()
    # print(sample.head())
    # print()
    predicted = model.predict(sample)[0]
    # print(predicted)
    
    # log the request
    structlog.get_logger().info(event="classify_email:predicted_class" , predicted_class=predicted)
    # return the response
    return jsonify({'predicted_class': predicted})
        

if __name__ == '__main__':

    # print ID of current process
    print("ID of process running main program: {}".format(os.getpid()))
    run_flask()
    
    
    