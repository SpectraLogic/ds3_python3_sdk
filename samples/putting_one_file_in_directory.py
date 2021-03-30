#   Copyright 2021 Spectra Logic Corporation. All Rights Reserved.
#   Licensed under the Apache License, Version 2.0 (the "License"). You may not use
#   this file except in compliance with the License. A copy of the License is located at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#   or in the "license" file accompanying this file.
#   This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
#   CONDITIONS OF ANY KIND, either express or implied. See the License for the
#   specific language governing permissions and limitations under the License.

import os

from ds3 import ds3, ds3Helpers

# This example puts ONE file /samples/resources/beowulf.txt to the bucket called books.
# This uses the new helper functions which creates and manages a single BP job.

# The bucket where to land the files.
bucket_name = "books"

# The file path being put to the BP.
file_path = os.path.join(os.path.dirname(str(__file__)), "resources", "beowulf.txt")

# Create a client which will be used to communicate with the BP.
client = ds3.createClientFromEnv()

# Make sure the bucket that we will be sending objects to exists
client.put_bucket(ds3.PutBucketRequest(bucket_name))

# Create the helper to gain access to the new data movement utilities.
helper = ds3Helpers.Helper(client=client)

# Create a HelperPutObject for each item you want to send to the BP.
# This example only puts one file, but you can send more than one at a time.
# For each object you must specify the name it will be called on the BP, the file path, and the size of the file.
file_size = os.path.getsize(file_path)
put_objects = [ds3Helpers.HelperPutObject(object_name="beowulf.txt", file_path=file_path, size=file_size)]

# Archive the files to the specified bucket
# You can optionally specify max_threads to tune performance.
put_job_id = helper.put_objects(put_objects=put_objects, bucket=bucket_name)
print("BP put job ID: " + put_job_id)

# we now verify that all our objects have been sent to DS3
bucketResponse = client.get_bucket(ds3.GetBucketRequest(bucket_name))

for obj in bucketResponse.result['ContentsList']:
    print(obj['Key'])
