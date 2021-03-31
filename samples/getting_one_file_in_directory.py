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

import tempfile

from os import path, walk
from ds3 import ds3, ds3Helpers

# This example gets ONE objects within the bucket books and lands it in a temp folder.
# This uses the new helper functions which creates and manages the BP job behind the scenes.
#
# This assumes that there exists a bucket called books on the BP and it contains the object beowulf.txt.
# Running the putting_one_file_in_directory.py example will create this setup.

# The bucket that contains the objects.
bucket_name = "books"

# The directory on the file system where the object will be landed.
# In this example, we are using a temporary directory for easy cleanup.
destination_directory = tempfile.TemporaryDirectory(prefix="books-dir")

# Create a client which will be used to communicate with the BP.
client = ds3.createClientFromEnv()

# Create the helper to gain access to the new data movement utilities.
helper = ds3Helpers.Helper(client=client)

# Create a HelperGetObject for each item you want to retrieve from the BP bucket.
# This example only gets one object, but you can transfer more than one at a time.
# For each object you must specify the name of the object on the BP, and the file path where you want to land the file.
# Optionally, if versioning is enabled on your bucket, you can specify the specific version to retrieve.
# If you don't specify a version, the most recent will be retrieved.
file_path = path.join(destination_directory.name, "beowulf.txt")
get_objects = [ds3Helpers.HelperGetObject(object_name="beowulf.txt", destination_path=file_path)]

# Retrieve the objects in the desired bucket.
# You can optionally specify max_threads to tune performance.
get_job_id = helper.get_objects(get_objects=get_objects, bucket=bucket_name)
print("BP get job ID: " + get_job_id)

# Verify that all the files have been landed in the folder.
for root, dirs, files in walk(top=destination_directory.name):
    for name in files:
        print("File: " + path.join(root, name))
    for name in dirs:
        print("Dir: " + path.join(root, name))

# Clean up the temp directory where we landed the files.
destination_directory.cleanup()
