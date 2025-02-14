import os
import tempfile
from ds3 import ds3
import time


# This example retrieves specified bytes=53687091100-53687091115 in the file in a specific bucket.
# The output will be written to tempname file.


client = ds3.createClientFromEnv()

#Change the following values to match your environment 
# Part size is the size of each upload part in bytes 
PART_SIZE = 5 * 1024 * 1024
BUCKET_NAME = "books" 
OBJECT_KEY = "beowulf.txt" 
FILE_PATH = "beowulf.txt"

#First step is to intiate the multipart upload. This will return uploadId which is used in the next request.
req = ds3.InitiateMultiPartUploadRequest(BUCKET_NAME, OBJECT_KEY)
res = client.initiate_multi_part_upload(req)
uploadId = res.result['UploadId']
file_size = os.path.getsize(FILE_PATH)
parts = []

with open(FILE_PATH, "rb") as file:
    part_number = 1
    while True:
        chunk = file.read(PART_SIZE)
        if not chunk:
            break

        req = ds3.PutMultiPartUploadPartRequest(
            bucket_name=BUCKET_NAME,
            object_name=OBJECT_KEY,
            part_number=part_number,
            upload_id=uploadId,
            request_payload=chunk
        )

        multi_res = client.put_multi_part_upload_part(req) 
        part_number += 1

print (res.result)