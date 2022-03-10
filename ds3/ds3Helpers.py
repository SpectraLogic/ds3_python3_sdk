#   Copyright 2021-2022 Spectra Logic Corporation. All Rights Reserved.
#   Licensed under the Apache License, Version 2.0 (the "License"). You may not use
#   this file except in compliance with the License. A copy of the License is located at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#   or in the "license" file accompanying this file.
#   This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
#   CONDITIONS OF ANY KIND, either express or implied. See the License for the
#   specific language governing permissions and limitations under the License.
import concurrent.futures
import hashlib
import time
import zlib
from os import walk, path
from platform import system
from typing import List, Set, Dict

from .ds3 import *

crc_byte_length = 4


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


def calculate_checksum_header(object_data_stream, checksum_type: str, length: int):
    bytes_read = 0
    if checksum_type == 'CRC_32':
        checksum = 0
        while bytes_read < length:
            bytes_to_read = min(1024 * 1024, length - bytes_read)
            cur_bytes = object_data_stream.read(bytes_to_read)
            checksum = zlib.crc32(cur_bytes, checksum)
            bytes_read += bytes_to_read
        encoded_checksum = base64.b64encode(
            checksum.to_bytes(crc_byte_length, byteorder='big')).decode()
        return {'Content-CRC32': encoded_checksum}
    else:
        if checksum_type == 'MD5':
            checksum_calculator = hashlib.md5()
            header_key = 'Content-MD5'
        elif checksum_type == 'SHA_256':
            checksum_calculator = hashlib.sha256()
            header_key = 'Content-SHA256'
        elif checksum_type == 'SHA_512':
            checksum_calculator = hashlib.sha512()
            header_key = 'Content-SHA512'
        else:
            raise 'Not Implemented: calculating checksum type {0} is not currently supported in the SDK helpers'.format(
                checksum_type)
        while bytes_read < length:
            bytes_to_read = min(1024 * 1024, length - bytes_read)
            cur_bytes = object_data_stream.read(bytes_to_read)
            checksum_calculator.update(cur_bytes)
            bytes_read += bytes_to_read
        encoded_checksum = base64.b64encode(checksum_calculator.digest()).decode('utf-8')
        return {header_key: encoded_checksum}


class Helper(object):
    """A class that moves data to and from a Black Pearl"""

    def __init__(self, client: Client, retry_delay_in_seconds: int = 60):
        """
        Parameters
        ----------
        client : ds3.Client
            A python client that is connected to a Black Pearl.
        retry_delay_in_seconds : int
            The number of seconds to wait between retrying a call if the Black Pearl is busy and unable to process the
            previous attempt (aka when BP returns error code 307).
        """
        self.client = client
        self.retry_delay_in_seconds = retry_delay_in_seconds

    def get_checksum_type(self, bucket_name: str) -> str:
        bucket_response = self.client.get_bucket_spectra_s3(GetBucketSpectraS3Request(bucket_name=bucket_name))

        data_policy_id = bucket_response.result['DataPolicyId']
        policy_response = self.client.get_data_policy_spectra_s3(
            GetDataPolicySpectraS3Request(data_policy_id=data_policy_id))

        return policy_response.result['ChecksumType']

    def put_objects(self, put_objects: List[HelperPutObject], bucket: str, max_threads: int = 5,
                    calculate_checksum: bool = False, job_name: str = None) -> str:
        """
        Puts a list of objects to a Black Pearl bucket.

        Parameters
        ----------
        put_objects : List[HelperPutObject]
            The list of objects to put into the BP bucket.
        bucket : str
            The name of the bucket where the objects are being landed.
        max_threads : int
            The number of concurrent objects being transferred at once (default 5).
        calculate_checksum : bool
            Whether the client calculates the object checksum before sending it to the BP (default False). The BP
            also calculates the checksum and compares it with the value the client calculates. The object put will fail
            if the client and BP checksums do not match. Note that calculating the checksum is processor intensive, and
            it also requires two reads of the object (first to calculate checksum, and secondly to send the data). The
            type of checksum calculated is determined by the data policy associated with the bucket.
        job_name : str
            The name to give the BP put job.
        """
        # If calculating checksum, then determine the checksum type from the data policy
        checksum_type = None
        if calculate_checksum is True:
            checksum_type = self.get_checksum_type(bucket_name=bucket)

        ds3_put_objects: List[Ds3PutObject] = []
        put_objects_map: Dict[str, HelperPutObject] = dict()
        for entry in put_objects:
            ds3_put_objects.append(Ds3PutObject(name=entry.object_name, size=entry.size))
            put_objects_map[entry.object_name] = entry

        bulk_put = self.client.put_bulk_job_spectra_s3(
            PutBulkJobSpectraS3Request(bucket_name=bucket, object_list=ds3_put_objects, name=job_name))

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

                            executor.submit(self.put_blob, bucket, put_object, cur_blob.length, cur_blob.offset, job_id,
                                            checksum_type)

        return job_id

    def put_blob(self, bucket: str, put_object: HelperPutObject, length: int, offset: int, job_id: str,
                 checksum_type: str = None):
        headers = None
        if checksum_type is not None:
            checksum_stream = put_object.get_data_stream(offset=offset)
            headers = calculate_checksum_header(object_data_stream=checksum_stream,
                                                checksum_type=checksum_type,
                                                length=length)
            checksum_stream.close()

        stream = put_object.get_data_stream(offset)
        self.client.put_object(PutObjectRequest(bucket_name=bucket,
                                                object_name=put_object.object_name,
                                                length=length,
                                                stream=stream,
                                                offset=offset,
                                                job=job_id,
                                                headers=headers))
        stream.close()

    def put_all_objects_in_directory(self, source_dir: str, bucket: str, objects_per_bp_job: int = 1000,
                                     max_threads: int = 5, calculate_checksum: bool = False,
                                     job_name: str = None) -> List[str]:
        """
        Puts all files and subdirectories to a Black Pearl bucket.

        Parameters
        ----------
        source_dir : str
            The directory that contains all the files and subdirectories to be put to the BP. Note that subdirectories
            will be represented by zero length objects whose name ends with the file path separator.
        bucket : str
            The name of the bucket where the files are being landed.
        objects_per_bp_job : int
            The number of objects per BP job (default 1000). A directory may contain more objects than a BP job can hold
            (max 500,000). In order to put all objects in a very large directory, multiple BP jobs may need to be
            created. This determines how many objects to bundle per BP job.
        max_threads : int
            The number of concurrent objects being transferred at once (default 5).
        calculate_checksum : bool
            Whether the client calculates the object checksum before sending it to the BP. The BP also calculates
            the checksum and compares it with the value the client calculates. The object put will fail if the client
            and BP checksums do not match. Note that calculating the checksum is processor intensive, and it also
            requires two reads of the object (first to calculate checksum, and secondly to send the data). The type of
            checksum calculated is determined by the data policy associated with the bucket.
        job_name : str
            The name to give the BP put jobs. All BP jobs that are created will have the same name.
        """
        obj_list: List[HelperPutObject] = list()
        job_list: List[str] = list()
        for root, dirs, files in walk(top=source_dir):
            for name in files:
                obj_path = path.join(root, name)
                obj_name = file_path_to_object_store_name(path.normpath(path.relpath(path=obj_path, start=source_dir)))
                size = os.path.getsize(obj_path)
                obj_list.append(HelperPutObject(object_name=obj_name, file_path=obj_path, size=size))
                if len(obj_list) >= objects_per_bp_job:
                    job_list.append(self.put_objects(obj_list, bucket, max_threads=max_threads,
                                                     calculate_checksum=calculate_checksum, job_name=job_name))
                    obj_list = []

            for name in dirs:
                dir_path = path.join(root, name)
                dir_name = file_path_to_object_store_name(
                    path.join(path.normpath(path.relpath(path=dir_path, start=source_dir)), ""))
                obj_list.append(HelperPutObject(object_name=dir_name, file_path=dir_path, size=0))
                if len(obj_list) >= objects_per_bp_job:
                    job_list.append(self.put_objects(obj_list, bucket, max_threads=max_threads,
                                                     calculate_checksum=calculate_checksum, job_name=job_name))
                    obj_list = []

        if len(obj_list) > 0:
            job_list.append(self.put_objects(
                obj_list, bucket, max_threads=max_threads, calculate_checksum=calculate_checksum, job_name=job_name))

        return job_list

    def get_objects(self, get_objects: List[HelperGetObject], bucket: str, max_threads: int = 5,
                    job_name: str = None) -> str:
        """
        Retrieves a list of objects from a Black Pearl bucket.

        Parameters
        ----------
        get_objects : List[HelperGetObject]
            The list of objects to retrieve from the BP bucket.
        bucket : str
            The name of the bucket where the objects are being retrieved from.
        max_threads : int
            The number of concurrent objects being transferred at once (default 5).
        job_name : str
            The name to give the BP get job.
        """
        ds3_get_objects: List[Ds3GetObject] = []
        get_objects_map: Dict[str, HelperGetObject] = dict()
        for entry in get_objects:
            ds3_get_objects.append(Ds3GetObject(name=entry.object_name, version_id=entry.version_id))
            get_objects_map[entry.object_name] = entry

        bulk_get = self.client.get_bulk_job_spectra_s3(GetBulkJobSpectraS3Request(bucket_name=bucket,
                                                                                  object_list=ds3_get_objects,
                                                                                  name=job_name))

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
                                max_threads: int = 5, job_name: str = None) -> List[str]:
        """
        Retrieves all objects from a Black Pearl bucket.

        Parameters
        ----------
        destination_dir : str
            The directory where all the objects will be landed.
        bucket : str
            The name of the bucket where the objects are being retrieved from.
        objects_per_bp_job : int
            The number of objects per BP job (default 1000). A bucket may contain more objects than a BP job can hold
            (max 500,000). In order to put all objects in a very large bucket, multiple BP jobs may need to be created.
            This determines how many objects to bundle per BP job.
        max_threads : int
            The number of concurrent objects being transferred at once (default 5).
        job_name : str
            The name to give the BP get jobs. All BP jobs that are created will have the same name.
        """
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
                job_id = self.get_objects(get_objects=get_objects, bucket=bucket, max_threads=max_threads,
                                          job_name=job_name)
                job_ids.append(job_id)

            truncated = list_bucket.result['IsTruncated']
            marker = list_bucket.result['NextMarker']

        return job_ids
