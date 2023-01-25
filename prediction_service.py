# CS 4981 ML Production Systems Project 4
# File is used for Offline Model Development and Prediction Service


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


def check_for_file_s3(file_name: str, bucket_name="joined-out"):
    # return s3_client.head_object(Bucket=bucket_name, Key=file_name)['ContentLength']
    try:
        return s3_resource.Object(bucket_name, file_name).content_length
    except ClientError:
        return -1