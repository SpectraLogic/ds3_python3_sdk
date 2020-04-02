#   Copyright 2014-2017 Spectra Logic Corporation. All Rights Reserved.
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
import tempfile
import time

from ds3 import ds3

# This example retrieves all objects in the specified bucket and lands them in the specified destination.
# By default it looks for objects in bucket 'books' and lands them in the temporary directory.
# At the end of running, those files are removed from the local system for testing reasons.
#
# This example assumes that a bucket named "books" containing some objects exist on the server.

bucketName = "books"  # modify this value to match the BP bucket you wish to retrieve objects from

destination = tempfile.gettempdir()  # modify this value to match where the object should be landed on your system

client = ds3.createClientFromEnv()

# retrieves a list of all objects in the bucket
bucketContents = client.get_bucket(ds3.GetBucketRequest(bucketName))

# Converting that list of objects into a list of objects to retrieve.
# If you want to retrieve a subset of objects, or already know their names, then just make a list of ds3.DesGetObject
# where each item describes one object you wish to retrieve from the BP.
objectList = list([ds3.Ds3GetObject(obj['Key']) for obj in bucketContents.result['ContentsList']])

# Create a dictionary to map the BP object name to the destination where your landing the object.
# In this example, we are landing all objects to the path described in the destination variable.
# Also, if the object name contains paths, this will normalize it for your OS and land that object
# in a sub-folder of the destination.
objectNameToDestinationPathMap = {}
for obj in objectList:
    objectNameToDestinationPathMap[obj.name] = os.path.join(destination, os.path.normpath(obj.name))

# Create a bulk get job on the BP. This tells the BP what objects your going to retrieve.
# This triggers the BP to start staging the objects in cache.
# Large objects may have been broken up into several pieces, i.e. blobs.
# The BP breaks up your retrieval job into "chunks".
# These chunks represent bundles of data that are ready to be retrieved.
# Each chunk which will contain one or more pieces of your files (blobs).
# How the job will be broken up (chunked) is determined when you create the bulk get job.
bulkGetResult = client.get_bulk_job_spectra_s3(ds3.GetBulkJobSpectraS3Request(bucketName, objectList))

# Create a set of the chunk ids that describe all units of work that make up the get job.
# This will be used to track which chunks we still need to process.
chunkIds = set([x['ChunkId'] for x in bulkGetResult.result['ObjectsList']])

# Attempt to retrieve data from the BP while there are still chunks that need to be processed.
while len(chunkIds) > 0:
    # Get a list of chunks for this job that are ready to be retrieved.
    availableChunks = client.get_job_chunks_ready_for_client_processing_spectra_s3(
        ds3.GetJobChunksReadyForClientProcessingSpectraS3Request(bulkGetResult.result['JobId']))

    chunks = availableChunks.result['ObjectsList']

    # Check to make sure we got some chunks, if we did not sleep and retry.
    # Having no chunks ready may indicate that the BP cache is currently full.
    if len(chunks) == 0:
        time.sleep(availableChunks.retryAfter)
        continue

    # For each chunk that is available, check to make sure we haven't processed it already.
    # If we have not processed this chunk yet, then retrieve all its objects.
    for chunk in chunks:
        if not chunk['ChunkId'] in chunkIds:
            # This chunk has already been processed
            continue

        # For each blob within this chunk, retrieve the data and land it on the destination.
        for obj in chunk['ObjectList']:
            # Open the destination file and seek to the offset corresponding with this blob.
            objectStream = open(objectNameToDestinationPathMap[obj['Name']], "wb")
            objectStream.seek(int(obj['Offset']))

            # Get the blob for the current object and write it to the destination.
            client.get_object(ds3.GetObjectRequest(bucketName,
                                                   obj['Name'],
                                                   objectStream,
                                                   offset=int(obj['Offset']),
                                                   job=bulkGetResult.result['JobId']))

            # Close the file handle.
            objectStream.close()

        # We've finished processing this chunk. Remove it from our list of chunks that still need processing.
        chunkIds.remove(chunk['ChunkId'])

# Go through all items that were landed and check that they were created.
# This is not needed in production code.
for objName in objectNameToDestinationPathMap.keys():
    destinationPath = objectNameToDestinationPathMap[objName]
    if os.path.isfile(destinationPath):
        fileSize = os.path.getsize(destinationPath)
        print(f'Retrieved object={objName}, landed at destination={destinationPath}, has size={fileSize}')

        # This removes the retrieved file from the destination.
        # This is done to clean up the script for when people are using it to test connection only.
        os.remove(destinationPath)  # Remove in production code.
    else:
        print(f'Failed to retrieve object={objName}')
