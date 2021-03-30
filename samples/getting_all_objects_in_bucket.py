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

# This example gets ALL objects within the bucket books and lands them in a temp folder.
# This uses the new helper functions which creates and manages the BP jobs behind the scenes.
#
# This assumes that there exists a bucket called books on the BP and it contains objects.
# Running the putting_all_files_in_directory.py example will create this setup.

# The bucket that contains the objects.
bucket_name = "books"

# The directory on the file system where the objects will be landed.
# In this example, we are using a temporary directory for easy cleanup.
destination_directory = tempfile.TemporaryDirectory(prefix="books-dir")

# Create a client which will be used to communicate with the BP.
client = ds3.createClientFromEnv()

# Create the helper to gain access to the new data movement utilities.
helper = ds3Helpers.Helper(client=client)

# Retrieve all the objects in the desired bucket and land them in the specified directory.
#
# You can optionally specify a objects_per_bp_job and max_threads to tune performance.
get_job_ids = helper.get_all_files_in_bucket(destination_dir=destination_directory.name, bucket=bucket_name)
print("BP get job IDS: " + get_job_ids.__str__())

# Verify that all the files have been landed in the folder.
for root, dirs, files in walk(top=destination_directory.name):
    for name in files:
        print("File: " + path.join(root, name))
    for name in dirs:
        print("Dir: " + path.join(root, name))

# Clean up the temp directory where we landed the files.
destination_directory.cleanup()
