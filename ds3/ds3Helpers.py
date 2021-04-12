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

import time
import concurrent.futures
from .ds3 import *
from os import walk, path
from typing import List, Set, Dict

from platform import system


class EmptyReader(object):
    @staticmethod
    def read(_):
        return None

    @staticmethod
    def close():
        return


class Blob(object):
    def __init__(self, name: str, length: int, offset: int):
        self.name = name
        self.length = length
        self.offset = offset

    def __eq__(self, other):
        if self.name == other.name and self.length == other.length and self.offset == other.offset:
            return True
        else:
            return False

    def __hash__(self):
        return hash((self.name, self.length, self.offset))


class HelperPutObject(object):
    def __init__(self, object_name: str, file_path: str, size: int):
        self.object_name = object_name
        self.file_path = file_path
        self.size = size

    def get_data_stream(self, offset: int):
        if self.size == 0:
            return EmptyReader()
        data_stream = open(self.file_path, "rb")
        data_stream.seek(offset, 0)
        return data_stream


class HelperGetObject(object):
    def __init__(self, object_name: str, destination_path: str, version_id: str = None):
        self.object_name = object_name
        self.destination_path = destination_path
        self.version_id = version_id

    def get_data_stream(self, offset: int):
        landing_dir = os.path.dirname(self.destination_path)
        if not os.path.exists(landing_dir):
            os.makedirs(name=landing_dir, exist_ok=True)

        fd = os.open(self.destination_path, os.O_CREAT | os.O_WRONLY)
        data_stream = os.fdopen(fd, 'wb')
        data_stream.seek(offset, 0)
        return data_stream


def file_path_to_object_store_name(file_path: str) -> str:
    if system().lower() == "windows":
        return file_path.replace('\\', '/')
    return file_path


def object_name_to_file_path(object_name: str) -> str:
    if system().lower() == "windows":
        return object_name.replace('/', '\\')
    return object_name


class Helper(object):
    def __init__(self, client: Client, retry_delay_in_seconds: int = 60):
        self.client = client
        self.retry_delay_in_seconds = retry_delay_in_seconds

    def put_objects(self, put_objects: List[HelperPutObject], bucket: str, max_threads: int = 5) -> str:
        ds3_put_objects: List[Ds3PutObject] = []
        put_objects_map: Dict[str, HelperPutObject] = dict()
        for entry in put_objects:
            ds3_put_objects.append(Ds3PutObject(name=entry.object_name, size=entry.size))
            put_objects_map[entry.object_name] = entry

        bulk_put = self.client.put_bulk_job_spectra_s3(
            PutBulkJobSpectraS3Request(bucket_name=bucket, object_list=ds3_put_objects))

        job_id = bulk_put.result['JobId']

        blob_set: Set[Blob] = set()
        for chunk in bulk_put.result['ObjectsList']:
            for blob in chunk['ObjectList']:
                name: str = blob['Name']
                length: int = int(blob['Length'])
                offset: int = int(blob['Offset'])
                cur_blob = Blob(name=name, length=length, offset=offset)
                blob_set.add(cur_blob)

        # send until all blobs have been transferred
        while len(blob_set) > 0:
            available_chunks = self.client.get_job_chunks_ready_for_client_processing_spectra_s3(
                GetJobChunksReadyForClientProcessingSpectraS3Request(job_id))

            chunks = available_chunks.result['ObjectsList']

            if len(chunks) <= 0:
                time.sleep(self.retry_delay_in_seconds)
                continue

            # retrieve all available blobs concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
                for chunk in chunks:
                    for blob in chunk['ObjectList']:
                        name: str = blob['Name']
                        length: int = int(blob['Length'])
                        offset: int = int(blob['Offset'])
                        cur_blob = Blob(name=name, length=length, offset=offset)

                        if cur_blob in blob_set:
                            blob_set.remove(cur_blob)
                            put_object = put_objects_map[cur_blob.name]

                            executor.submit(self.put_blob, bucket, put_object, cur_blob.length, cur_blob.offset, job_id)

        return job_id

    def put_blob(self, bucket: str, put_object: HelperPutObject, length: int, offset: int, job_id: str):
        stream = put_object.get_data_stream(offset)
        self.client.put_object(PutObjectRequest(bucket_name=bucket,
                                                object_name=put_object.object_name,
                                                length=length,
                                                stream=stream,
                                                offset=offset,
                                                job=job_id))
        stream.close()

    def put_all_objects_in_directory(self, source_dir: str, bucket: str, objects_per_bp_job: int = 1000,
                                     max_threads: int = 5) -> List[str]:
        obj_list: List[HelperPutObject] = list()
        job_list: List[str] = list()
        for root, dirs, files in walk(top=source_dir):
            for name in files:
                obj_path = path.join(root, name)
                obj_name = file_path_to_object_store_name(path.normpath(path.relpath(path=obj_path, start=source_dir)))
                size = os.path.getsize(obj_path)
                obj_list.append(HelperPutObject(object_name=obj_name, file_path=obj_path, size=size))
                if len(obj_list) >= objects_per_bp_job:
                    job_list.append(self.put_objects(obj_list, bucket, max_threads=max_threads))
                    obj_list = []

            for name in dirs:
                dir_path = path.join(root, name)
                dir_name = file_path_to_object_store_name(
                    path.join(path.normpath(path.relpath(path=dir_path, start=source_dir)), ""))
                obj_list.append(HelperPutObject(object_name=dir_name, file_path=dir_path, size=0))
                if len(obj_list) >= objects_per_bp_job:
                    job_list.append(self.put_objects(obj_list, bucket, max_threads=max_threads))
                    obj_list = []

        if len(obj_list) > 0:
            job_list.append(self.put_objects(obj_list, bucket, max_threads=max_threads))

        return job_list

    def get_objects(self, get_objects: List[HelperGetObject], bucket: str, max_threads: int = 5) -> str:
        ds3_get_objects: List[Ds3GetObject] = []
        get_objects_map: Dict[str, HelperGetObject] = dict()
        for entry in get_objects:
            ds3_get_objects.append(Ds3GetObject(name=entry.object_name, version_id=entry.version_id))
            get_objects_map[entry.object_name] = entry

        bulk_get = self.client.get_bulk_job_spectra_s3(GetBulkJobSpectraS3Request(bucket_name=bucket,
                                                                                  object_list=ds3_get_objects))

        job_id = bulk_get.result['JobId']

        blob_set: Set[Blob] = set()
        for chunk in bulk_get.result['ObjectsList']:
            for blob in chunk['ObjectList']:
                name: str = blob['Name']
                length: int = int(blob['Length'])
                offset: int = int(blob['Offset'])
                cur_blob = Blob(name=name, length=length, offset=offset)
                blob_set.add(cur_blob)

        # retrieve until all blobs have been transferred
        while len(blob_set) > 0:
            available_chunks = self.client.get_job_chunks_ready_for_client_processing_spectra_s3(
                GetJobChunksReadyForClientProcessingSpectraS3Request(job_id))

            chunks = available_chunks.result['ObjectsList']

            if len(chunks) <= 0:
                time.sleep(self.retry_delay_in_seconds)
                continue

            # retrieve all available blobs concurrently
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
                for chunk in chunks:
                    for blob in chunk['ObjectList']:
                        name: str = blob['Name']
                        length: int = int(blob['Length'])
                        offset: int = int(blob['Offset'])
                        cur_blob = Blob(name=name, length=length, offset=offset)

                        if cur_blob in blob_set:
                            blob_set.remove(cur_blob)
                            get_object = get_objects_map[cur_blob.name]

                            executor.submit(self.get_blob, bucket, get_object, offset, job_id)

        return job_id

    def get_blob(self, bucket: str, get_object: HelperGetObject, offset: int, job_id: str):
        stream = get_object.get_data_stream(offset)
        self.client.get_object(GetObjectRequest(bucket_name=bucket,
                                                object_name=get_object.object_name,
                                                stream=stream,
                                                offset=offset,
                                                job=job_id,
                                                version_id=get_object.version_id))
        stream.close()

    def get_all_files_in_bucket(self, destination_dir: str, bucket: str, objects_per_bp_job: int = 1000,
                                max_threads: int = 5) -> List[str]:
        truncated: str = 'true'
        marker = ""
        job_ids: List[str] = []
        while truncated.lower() == 'true':
            list_bucket = self.client.get_bucket(GetBucketRequest(bucket_name=bucket,
                                                                  max_keys=objects_per_bp_job,
                                                                  versions=False,
                                                                  marker=marker))

            get_objects: List[HelperGetObject] = []
            for bp_object in list_bucket.result['ContentsList']:
                is_latest: str = bp_object['IsLatest']
                if is_latest.lower() != 'true':
                    # only retrieve the latest version of objects
                    continue

                object_name: str = bp_object["Key"]
                object_destination = os.path.join(destination_dir, object_name_to_file_path(object_name))
                if object_name.endswith('/'):
                    os.makedirs(object_destination, exist_ok=True)
                else:
                    get_objects.append(HelperGetObject(object_name=object_name, destination_path=object_destination))

            for bp_object in list_bucket.result['VersionList']:
                is_latest: str = bp_object['IsLatest']
                if is_latest.lower() != 'true':
                    # only retrieve the latest version of objects
                    continue

                object_name: str = bp_object["Key"]
                object_destination = os.path.join(destination_dir, object_name_to_file_path(object_name))
                if object_name.endswith('/'):
                    os.makedirs(object_destination, exist_ok=True)
                else:
                    get_objects.append(HelperGetObject(object_name=object_name, destination_path=object_destination))

            if len(get_objects) > 0:
                job_id = self.get_objects(get_objects=get_objects, bucket=bucket, max_threads=max_threads)
                job_ids.append(job_id)

            truncated = list_bucket.result['IsTruncated']
            marker = list_bucket.result['NextMarker']

        return job_ids
