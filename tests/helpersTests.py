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
import logging
import unittest
import os
import tempfile
import uuid
import concurrent.futures

from ds3 import ds3
from ds3 import ds3Helpers
from typing import List, Dict

import xml.etree.ElementTree as xmlDom

logging.basicConfig(level=logging.INFO)


def create_files_in_directory(directory: str, num_files: int, root_dir: str,
                              include_dirs: bool = True) -> List[ds3Helpers.HelperPutObject]:
    put_objects = []
    # create the directory if it doesn't exist
    if not os.path.exists(directory):
        os.mkdir(path=directory)
        if include_dirs:
            obj_name = ds3Helpers.file_path_to_object_store_name(os.path.join(os.path.relpath(directory, root_dir), ""))
            put_objects.append(ds3Helpers.HelperPutObject(object_name=obj_name, file_path=directory, size=0))

    # create an empty subdirectory
    if include_dirs:
        dir_path = os.path.join(directory, 'empty-dir')
        os.mkdir(path=dir_path)
        obj_name = ds3Helpers.file_path_to_object_store_name(os.path.join(os.path.relpath(dir_path, root_dir), ""))
        put_objects.append(ds3Helpers.HelperPutObject(object_name=obj_name, file_path=directory, size=0))

    # create some files
    for i in range(num_files):
        file_path = os.path.join(directory, f'file-{i}.txt')
        f = open(file_path, "a")
        f.write(f'I am file number {i}')
        f.close()

        obj_name = ds3Helpers.file_path_to_object_store_name(os.path.relpath(file_path, root_dir))
        size = os.path.getsize(file_path)
        put_objects.append(ds3Helpers.HelperPutObject(object_name=obj_name, file_path=file_path, size=size))

    return put_objects


class Ds3HelpersTestCase(unittest.TestCase):
    def test_file_path_to_object_store_name(self):
        self.assertEqual(ds3Helpers.file_path_to_object_store_name(os.path.join("some", "dir", "")), 'some/dir/')
        self.assertEqual(ds3Helpers.file_path_to_object_store_name(os.path.join("some", "file")), 'some/file')

    def test_marshaling_put_object_list(self):
        dir_obj = ds3.Ds3PutObject(name="dir-0/", size=0)
        object_list: List[ds3.Ds3PutObject] = [dir_obj]
        xml_object_list = ds3.Ds3PutObjectList(object_list)
        to_xml = xml_object_list.to_xml()
        result = xmlDom.tostring(to_xml)
        self.assertEqual(result, b'<Objects><Object Name="dir-0/" Size="0" /></Objects>')

    @staticmethod
    def write_to_stream(i: int, char: str, get_object: ds3Helpers.HelperGetObject):
        offset = i * 10
        content = ''
        for j in range(10):
            content += char
        stream = get_object.get_data_stream(offset)
        stream.write(bytes(content, 'utf-8'))
        stream.close()

    def test_get_object_data_stream(self):
        directory = tempfile.TemporaryDirectory(prefix="ds3-python3-sdk-")
        file_path = os.path.join(directory.name, "sub-dir", "file.txt")

        get_object = ds3Helpers.HelperGetObject(object_name="file.txt", destination_path=file_path)

        inputs = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i']
        expected: str = ''
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            for i in range(len(inputs)):
                for j in range(10):
                    expected += inputs[i]
                executor.submit(self.write_to_stream, i, inputs[i], get_object)

        file = open(file_path)
        content = file.read()
        self.assertEqual(expected, content)
        file.close()

        directory.cleanup()

    def test_put_and_get_objects(self):
        bucket = f'ds3-python3-sdk-test-{uuid.uuid1()}'

        # create temporary directory with some files
        source = tempfile.TemporaryDirectory(prefix="ds3-python3-sdk-src-")
        put_objects = create_files_in_directory(directory=source.name,
                                                num_files=10,
                                                root_dir=source.name,
                                                include_dirs=False)

        # create the BP helper and perform the put all objects call
        job_name = "python test job"
        client = ds3.createClientFromEnv()
        client.put_bucket_spectra_s3(ds3.PutBucketSpectraS3Request(name=bucket))

        helpers = ds3Helpers.Helper(client=client)
        job_id = helpers.put_objects(bucket=bucket, put_objects=put_objects, job_name=job_name)
        self.assertNotEqual(job_id, "", "job id was returned")

        # verify all the files and directories are on the BP
        head_obj = client.head_object(ds3.HeadObjectRequest(bucket_name=bucket, object_name="does-not-exist"))
        self.assertEqual(head_obj.result, "DOESNTEXIST")

        for put_object in put_objects:
            head_obj = client.head_object(ds3.HeadObjectRequest(bucket_name=bucket, object_name=put_object.object_name))
            self.assertNotEqual(head_obj.result, "DOESNTEXIST")

        # verify that the job was created with the desired name
        get_job = client.get_job_spectra_s3(ds3.GetJobSpectraS3Request(job_id=job_id))
        self.assertEqual(get_job.result['Name'], job_name)

        # retrieve the files from the BP
        destination = tempfile.TemporaryDirectory(prefix="ds3-python3-sdk-dst-")
        get_objects: List[ds3Helpers.HelperGetObject] = []
        object_name_to_source: Dict[str, str] = dict()
        for put_object in put_objects:
            destination_path = os.path.join(destination.name, put_object.object_name)
            get_objects.append(
                ds3Helpers.HelperGetObject(object_name=put_object.object_name, destination_path=destination_path))
            object_name_to_source[put_object.object_name] = put_object.file_path

        # perform the get objects call
        job_id = helpers.get_objects(bucket=bucket, get_objects=get_objects, job_name=job_name)
        self.assertNotEqual(job_id, "", "job id was returned")

        for get_object in get_objects:
            original_file = open(object_name_to_source[get_object.object_name], 'rb')
            retrieved_file = open(get_object.destination_path, 'rb')

            original_content = original_file.read()
            retrieved_content = retrieved_file.read()
            self.assertEqual(original_content, retrieved_content)
            original_file.close()
            retrieved_file.close()

        # verify that the job was created with the desired name
        get_job = client.get_job_spectra_s3(ds3.GetJobSpectraS3Request(job_id=job_id))
        self.assertEqual(get_job.result['Name'], job_name)

        # cleanup
        source.cleanup()
        destination.cleanup()
        client.delete_bucket_spectra_s3(ds3.DeleteBucketSpectraS3Request(bucket_name=bucket, force=True))

    def test_put_and_get_all_objects_in_directory(self):
        bucket = f'ds3-python3-sdk-test-{uuid.uuid1()}'
        job_name = "python test job"

        # create temporary directory with some files and subdirectories
        source = tempfile.TemporaryDirectory(prefix="ds3-python3-sdk-src-")

        put_objects = create_files_in_directory(directory=source.name, num_files=5, root_dir=source.name)
        for i in range(2):
            sub_dir_path = os.path.join(source.name, f'dir-{i}')
            put_objects += create_files_in_directory(directory=sub_dir_path, num_files=2, root_dir=source.name)
            for j in range(2):
                sub_sub_dir_path = os.path.join(sub_dir_path, f'sub-dir-{j}')
                put_objects += create_files_in_directory(directory=sub_sub_dir_path,
                                                         num_files=2,
                                                         root_dir=source.name)

        # create the BP helper and perform the put all objects call
        client = ds3.createClientFromEnv()
        client.put_bucket(ds3.PutBucketRequest(bucket_name=bucket))

        helpers = ds3Helpers.Helper(client=client)
        job_ids = helpers.put_all_objects_in_directory(source_dir=source.name, bucket=bucket, objects_per_bp_job=10,
                                                       job_name=job_name)
        self.assertGreaterEqual(len(job_ids), 1, "received at least one job id")

        # verify all the files and directories are on the BP
        for put_object in put_objects:
            head_obj = client.head_object(ds3.HeadObjectRequest(bucket_name=bucket, object_name=put_object.object_name))
            self.assertNotEqual(head_obj.result, "DOESNTEXIST")

        # verify that all the job were created with the desired name
        for job_id in job_ids:
            get_job = client.get_job_spectra_s3(ds3.GetJobSpectraS3Request(job_id=job_id))
            self.assertEqual(get_job.result['Name'], job_name)

        # retrieve the objects from the BP
        destination = tempfile.TemporaryDirectory(prefix="ds3-python3-sdk-dst-")
        job_ids = helpers.get_all_files_in_bucket(destination_dir=destination.name,
                                                  bucket=bucket,
                                                  objects_per_bp_job=10,
                                                  job_name=job_name)

        self.assertGreaterEqual(len(job_ids), 2, "multiple job ids returned")

        # verify all the files and directories were retrieved
        for put_object in put_objects:
            obj_destination = os.path.join(destination.name,
                                           ds3Helpers.object_name_to_file_path(put_object.object_name))
            if put_object.object_name.endswith('/'):
                self.assertTrue(os.path.isdir(obj_destination), f'expected path to be directory: {obj_destination}')
            else:
                self.assertTrue(os.path.isfile(obj_destination), f'expected path to be file: {obj_destination}')
                self.assertEqual(put_object.size, os.path.getsize(obj_destination), 'file size')

        # verify that all the job were created with the desired name
        for job_id in job_ids:
            get_job = client.get_job_spectra_s3(ds3.GetJobSpectraS3Request(job_id=job_id))
            self.assertEqual(get_job.result['Name'], job_name)

        # cleanup
        source.cleanup()
        destination.cleanup()
        client.delete_bucket_spectra_s3(ds3.DeleteBucketSpectraS3Request(bucket_name=bucket, force=True))

    def test_put_all_objects_in_directory_with_md5_checksum(self):
        self.put_all_objects_in_directory_with_checksum(checksum_type='MD5')

    def test_put_all_objects_in_directory_with_crc32_checksum(self):
        self.put_all_objects_in_directory_with_checksum(checksum_type='CRC_32')

    def test_put_all_objects_in_directory_with_sha_256_checksum(self):
        self.put_all_objects_in_directory_with_checksum(checksum_type='SHA_256')

    def test_put_all_objects_in_directory_with_sha_512_checksum(self):
        self.put_all_objects_in_directory_with_checksum(checksum_type='SHA_512')

    def put_all_objects_in_directory_with_checksum(self, checksum_type: str):
        bucket = f'ds3-python3-sdk-test-{uuid.uuid1()}'
        # checksum_type = 'MD5'

        # create the BP client
        client = ds3.createClientFromEnv()

        # create a data policy
        data_policy = client.put_data_policy_spectra_s3(ds3.PutDataPolicySpectraS3Request(
            name=f'sdk-test-{checksum_type}', checksum_type=checksum_type, end_to_end_crc_required=True))
        data_policy_id = data_policy.result['Id']

        # fetch existing storage domain
        storage_domain = client.get_storage_domains_spectra_s3(ds3.GetStorageDomainsSpectraS3Request())
        storage_domain_id = storage_domain.result['StorageDomainList'][0]['Id']

        data_persistence_rule = client.put_data_persistence_rule_spectra_s3(
            ds3.PutDataPersistenceRuleSpectraS3Request(data_policy_id=data_policy_id, isolation_level='standard',
                                                       storage_domain_id=storage_domain_id, type='permanent'))

        # create temporary directory with some files and subdirectories
        source = tempfile.TemporaryDirectory(prefix="ds3-python3-sdk-src-")

        put_objects = create_files_in_directory(directory=source.name, num_files=5, root_dir=source.name,
                                                include_dirs=True)

        # create the BP helper and perform the put all objects call
        client.put_bucket_spectra_s3(ds3.PutBucketSpectraS3Request(name=bucket, data_policy_id=data_policy_id))

        helpers = ds3Helpers.Helper(client=client)
        job_ids = helpers.put_all_objects_in_directory(source_dir=source.name, bucket=bucket, calculate_checksum=True)
        self.assertGreaterEqual(len(job_ids), 1, "received at least one job id")

        # verify all the files and directories are on the BP
        for put_object in put_objects:
            head_obj = client.head_object(ds3.HeadObjectRequest(bucket_name=bucket, object_name=put_object.object_name))
            self.assertNotEqual(head_obj.result, "DOESNTEXIST")

        # cleanup
        source.cleanup()

        client.delete_bucket_spectra_s3(ds3.DeleteBucketSpectraS3Request(bucket_name=bucket, force=True))

        client.delete_data_persistence_rule_spectra_s3(
            ds3.DeleteDataPersistenceRuleSpectraS3Request(data_persistence_rule_id=data_persistence_rule.result['Id']))

        client.delete_data_policy_spectra_s3(
            ds3.DeleteDataPolicySpectraS3Request(data_policy_id=data_policy_id))


if __name__ == '__main__':
    unittest.main()
