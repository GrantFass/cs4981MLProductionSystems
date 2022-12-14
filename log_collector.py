from pygtail import Pygtail
import boto3
from minio import Minio
import os
from dotenv import load_dotenv

load_dotenv()
log_file = os.path.relpath('./log_file.json')

tail = Pygtail(log_file, save_on_end=True, copytruncate=False)
count = 0
temp = []
for line, offset in tail.with_offsets():
    temp.append(line)
    count = offset
print(temp)

# client = Minio(
#     os.getenv('ENDPOINT_URL'), 
#     os.getenv('AWS_ACCESS_KEY_ID'), 
#     os.getenv('AWS_SECRET_ACCESS_KEY'))
s3_target = boto3.resource('s3', 
    endpoint_url=os.getenv('ENDPOINT_URL'),
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    aws_session_token=None,
    config=boto3.session.Config(signature_version='s3v4'),
    verify=False
)
print(s3_target)
#tail.write_offset_to_file(count)

