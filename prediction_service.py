# CS 4981 ML Production Systems Project 4
# File is used for Offline Model Development and Prediction Service

import datetime
import json
import os
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


# set up the structured logging file
with open("log_file.json", "wt", encoding="utf-8") as log_fl:
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


def read_from_s3_iter(file_name: str, bucket_name="joined-out"):
    # https://stackoverflow.com/questions/36205481/read-file-content-from-s3-bucket-with-boto3
    bucket = s3_resource.Bucket(bucket_name)
    # Iterates through all the objects, doing the pagination for you. Each obj
    # is an ObjectSummary, so it doesn't contain the body. You'll need to call
    # get to get the whole body.
    found = False
    data = ""
    for obj in bucket.objects.all():
        key = obj.key
        if file_name in str(key):
            data = obj.get()['Body'].read()
            print("%s : %s" % (key, data))
            found = True
    if found:
        return data
    else:
        print("ERROR KEY NOT FOUND")
        return -1

def read_from_s3_bucket(file_name: str, bucket_name="joined-out"):
    # read all the files in the bucket
    bucket = s3_resource.Bucket(bucket_name)
    found = False
    data = ""
    for obj in bucket.objects.all():
        key = obj.key
        if file_name in str(key):
            body = obj.get()['Body'].read()
            
            if isinstance(body, bytes):
                body = body.decode()
            elif not isinstance(body, str):
                body = str(body)
                
            print("%s : length = %s" % (key, len(body)))
                
            data += body
            # data = body
            # data.append(body)
            found = True
    if found:
        return data
    else:
        print("ERROR KEY NOT FOUND")
        return -1


def check_for_file_s3(file_name: str, bucket_name="joined-out"):
    # return s3_client.head_object(Bucket=bucket_name, Key=file_name)['ContentLength']
    try:
        return s3_resource.Object(bucket_name, file_name).content_length
    except ClientError:
        return -1
    
    
def run_flask():
    with open("log_file.json", "wt", encoding="utf-8") as log_fl:
        structlog.configure(
            processors=[structlog.processors.TimeStamper(fmt="iso"),
                        structlog.processors.JSONRenderer()],
            logger_factory=structlog.WriteLoggerFactory(file=log_fl))
        app.run(debug=True, port=8888)
       

@app.route('/classify_email', methods=['POST'])
def classify_email():
    # get the data from the request
    data = request.data.decode('utf-8')
    data = json.loads(data)
    predicted = data['predicted_class']
    # log the request
    structlog.get_logger().info(event="classify_email:predicted_class" , predicted_class=predicted)
    # return the response
    return jsonify({'predicted_class': predicted})


def offline_model():
    DEBUG = True
    # read in all the data from S3 as one big string
    data = read_from_s3_bucket(file_name="out/")
    
    # Split the string of data into individual lines. Only works because backslash is escaped in string.
    data = data.split('\n')
    data = data[0:-1]
    if DEBUG: print(len(data))
    
    # convert every entry in the list to a JSON object. 
    records = []
    for i in range(len(data)):
        record = json.loads(data[i])
        record['email_object'] = json.loads(record['email_object'])
        record['to'] = record['email_object']['to']
        record['body'] = record['email_object']['body']
        record['from'] = record['email_object']['from']
        record['subject'] = record['email_object']['subject']
        records.append(record)
    
    if DEBUG: print("Type: %s of type: %s, Len %d" % (type(records), type(records[0]), len(records)))
    # if DEBUG: print(json.dumps(records[0]))

    # load the data into a dataframe and set proper types
    df = pd.DataFrame(records)
    df['received_timestamp'] = pd.to_datetime(df['received_timestamp'], utc=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    df['label'] = df['label'].astype('category')
    df['event'] = df['event'].astype('category')
    df = df.sort_values(by='received_timestamp')
    
    if DEBUG: print(df.head(5))
    if DEBUG: print(df.info())
    
    # Perform Time Based Split
    n = len(df)
    train_amt = int(0.7 * n)
    test_amt = int(0.2 * n)
    validation_amt = int(0.1 * n)
    # split the dataframe by slicing based on index. Only works due to being sorted by time.
    training_targets = ['to', 'from', 'body', 'subject']
    train_x = df.iloc[0:train_amt][training_targets]
    train_y = df.iloc[0:train_amt]['label']
    validation_x = df.iloc[train_amt:validation_amt + train_amt][training_targets]
    validation_y = df.iloc[train_amt:validation_amt + train_amt]['label']
    test_x = df.iloc[train_amt + validation_amt:n][training_targets]
    test_y = df.iloc[train_amt + validation_amt:n]['label']
    if DEBUG: print("Train Size: %d\tValidation Size: %d\tTest Size: %d" % (len(train_x), len(validation_x), len(test_x)))
    
    # Deal with Class Imbalance by undersampling the majority for training
    y_orig = train_y
    x_orig = train_x # truncated?
    if DEBUG: print('Original dataset shape {}'.format(Counter(y_orig)))
    rus = RandomUnderSampler(random_state=42)
    x, y = rus.fit_resample(x_orig, y_orig)
    if DEBUG: print('Resampled dataset shape {}'.format(Counter(y)))
    train_x = x
    train_y = pd.DataFrame(y)
    if DEBUG: print(train_y.shape)
    if DEBUG: print(train_x.shape)
    
    # Perform Quick Test
    
    vectorizer = CountVectorizer(binary=True)
    
    # x_train = vectorizer.fit_transform(train[training_targets])
    # x_validate = vectorizer.fit_transform(validation[training_targets])
    vect = TruncatedSVD()
    # truncated = vec.fit_transform(x)
    
    # # x_train = train.copy()[['to', 'from', 'body', 'subject']]
    # # y_train = train.copy()['label']
    # # x_test = validation.copy()[['to', 'from', 'body', 'subject']]
    # # y_test = validation.copy()['label']['to', 'from', 'body', 'subject']]
    
    # TODO: change to pipeline
    
    
    # train_vectorizer = vectorizer.fit_transform(x)
    # x_train = vec.fit_transform(train_vectorizer)
    # y_train = y
    
    # validation_vectorizer = vectorizer.fit_transform(validation_x)
    # x_test = vec.fit_transform(validation_vectorizer)
    # y_test = validation_yvectorizer = CountVectorizer(binary=True, min_df=10)
    
    
    x_train = vect.fit_transform(vectorizer.fit_transform(x['body']))
    y_train = y
    x_test = vect.fit_transform(vectorizer.fit_transform(validation_x['body']))
    y_test = validation_y
    
    print(x_train.shape)
    print(y_train.shape)
    
    clf = svm.SVC(kernel='linear')
    clf.fit(x_train, y_train)
    y_pred = clf.predict(x_test)
    y_decision = clf.decision_function(x_test)
    # print("Accuracy: %.2f" % (metrics.accuracy_score(y_test, y_pred)))
    # print("Precision: %.2f" % (metrics.precision_score(y_test, y_pred, average='weighted', zero_division=0)))
    # print("Recall: %.2f" % (metrics.recall_score(y_test, y_pred, average='weighted')))
    print("ROC_AUC: %.2f" % (metrics.roc_auc_score(y_test, y_decision)))
    svc_disp = metrics.RocCurveDisplay.from_estimator(clf, x_test, y_test)
    plt.show()
        


if __name__ == '__main__':

    # print ID of current process
    print("ID of process running main program: {}".format(os.getpid()))
    offline_model()
    # run_flask()
    