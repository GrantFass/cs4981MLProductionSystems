from pygtail import Pygtail
import boto3
import minio
import os
log_file = os.path.relpath('./log_file.json')

tail = Pygtail(log_file, save_on_end=True, copytruncate=False)
count = 0
temp = []
for line, offset in tail.with_offsets():
    temp.append(line)
    count = offset
print(temp)
# tail.write_offset_to_file(count)

