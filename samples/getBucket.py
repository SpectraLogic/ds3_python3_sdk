#   Copyright 2014-2020 Spectra Logic Corporation. All Rights Reserved.
#   Licensed under the Apache License, Version 2.0 (the "License"). You may not use
#   this file except in compliance with the License. A copy of the License is located at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#   or in the "license" file accompanying this file.
#   This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
#   CONDITIONS OF ANY KIND, either express or implied. See the License for the
#   specific language governing permissions and limitations under the License.

import sys

from ds3 import ds3

# take bucketname (required) and prefix (optional) from command line
if len(sys.argv) < 2:
    print('Usage: python getBuckey.py <bucketname> [prefix]')
    exit(0)

bucket = sys.argv[1]
prefix = None
if len(sys.argv) > 2:
    prefix = sys.argv[2]

# create a dictionary to contain object names
object_dict={}
object_dict[bucket]=[]

# get_bucket returns max 1000 objects -- use pagination to get more
getMore = True
marker = None

client = ds3.createClientFromEnv()

while getMore:
    # the call will return a list of 1000 bucket objects
    resp = client.get_bucket(ds3.GetBucketRequest(bucket, None, marker, 1000, prefix))

    # set pagination variables for next call
    getMore = resp.result["IsTruncated"] == 'true'
    marker = resp.result["NextMarker"]

    # extract what is wanted from get_bucket response; key is object name
    objectNames = [bucket['Key'] for bucket in resp.result['ContentsList']]
    for name in objectNames:
        object_dict[bucket].append(name)

# print object list in JSON
print(object_dict)
