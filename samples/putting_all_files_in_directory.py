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

# This example puts ALL files within the sub-folder /samples/resources to the bucket called books.
# This uses the new helper functions which creates and manages the BP jobs behind the scenes.

# The bucket where to land the files.
bucket_name = "books"

# The directory that contains files to be archived to BP.
# In this example, we are moving all files in the ds3_python3_sdk/samples/resources folder.
directory_with_files = os.path.join(os.path.dirname(str(__file__)), "resources")

# Create a client which will be used to communicate with the BP.
client = ds3.createClientFromEnv()

# Make sure the bucket that we will be sending objects to exists
client.put_bucket(ds3.PutBucketRequest(bucket_name))

# Create the helper to gain access to the new data movement utilities.
helper = ds3Helpers.Helper(client=client)

# Archive all the files in the desired directory to the specified bucket.
# Note that the file's object names will be relative to the root directory you specified.
# For example: resources/beowulf.txt will be named just beowulf.txt in the BP bucket.
#
# You can optionally specify a objects_per_bp_job and max_threads to tune performance.
put_job_ids = helper.put_all_objects_in_directory(source_dir=directory_with_files, bucket=bucket_name)
print("BP put job IDs: " + put_job_ids.__str__())

# we now verify that all our objects have been sent to DS3
bucketResponse = client.get_bucket(ds3.GetBucketRequest(bucket_name))

for obj in bucketResponse.result['ContentsList']:
    print(obj['Key'])
