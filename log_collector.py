from pygtail import Pygtail
import boto3
from minio import Minio
import os
from dotenv import load_dotenv
import json
from datetime import datetime, timedelta
from time import sleep
from sys import argv

load_dotenv()
diff = 1
try:
    diff = int(argv[1])
except IndexError:
    diff = 1

s3_target = boto3.resource('s3', 
    endpoint_url=os.getenv('ENDPOINT_URL'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=None,
    config=boto3.session.Config(signature_version='s3v4'),
    verify=False
)

mailbox_path_offset = os.getenv('MAILBOX_LOG_PATH_OFFSET')
if os.path.exists(mailbox_path_offset):
    os.remove(mailbox_path_offset)

prediction_path_offset = os.getenv('PREDICTION_LOG_PATH_OFFSET')
if os.path.exists(prediction_path_offset):
    os.remove(prediction_path_offset)
    
mailbox_path = os.getenv('MAILBOX_LOG_PATH')
prediction_path = os.getenv('PREDICTION_LOG_PATH')

print("starting logging")
while True:
    start = datetime.now()
    until = start + timedelta(minutes=diff)
    print("logging at %s" % until)
    while datetime.now() < until:
        sleep(1)
        
    # Upload the mailbox log file
    if os.path.exists(mailbox_path):
        tail = Pygtail(mailbox_path, save_on_end=True, copytruncate=False)
        temp_str = ""
        for line, offset in tail.with_offsets():
            j = json.loads(line)
            temp_str += json.dumps(j) + "\n"
            temp_str = temp_str.replace("\\u", "/u")
        if temp_str:
            print(temp_str)
            log_name = "log_file_%s.json" % datetime.now().strftime("%Y%m%d-%H%M%S")
            s3_target.Bucket('log-files').put_object(Key=log_name, Body=temp_str)
    
    # Upload the prediction log file
    if os.path.exists(prediction_path):
        tail = Pygtail(prediction_path, save_on_end=True, copytruncate=False)
        temp_str = ""
        for line, offset in tail.with_offsets():
            j = json.loads(line)
            temp_str += json.dumps(j) + "\n"
            temp_str = temp_str.replace("\\u", "/u")
        if temp_str:
            print(temp_str)
            log_name = "log_file_%s.json" % datetime.now().strftime("%Y%m%d-%H%M%S")
            s3_target.Bucket('prediction-log-files').put_object(Key=log_name, Body=temp_str)

