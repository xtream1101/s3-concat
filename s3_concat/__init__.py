import logging

from .utils import _create_s3_client, _convert_to_bytes, _chunk_by_size
from .multipart_upload_job import MultipartUploadJob

logger = logging.getLogger(__name__)


class S3Concat:

    def __init__(self, bucket, key, min_file_size,
                 content_type='application/octet-stream'):
        self.bucket = bucket
        self.key = key
        self.min_file_size = _convert_to_bytes(min_file_size)
        self.content_type = content_type
        self.all_files = []
        self.s3 = _create_s3_client()

    def concat(self, small_parts_threads=1):

        grouped_parts_list = _chunk_by_size(self.all_files, self.min_file_size)
        logger.info("Created {} concatenation groups"
                    .format(len(grouped_parts_list)))

        for part_data in grouped_parts_list:
            MultipartUploadJob(self.bucket, self.key, part_data,
                               small_parts_threads=small_parts_threads,
                               content_type=self.content_type)

    def add_files(self, prefix):

        def resp_to_filelist(resp):
            return [(x['Key'], x['Size']) for x in resp['Contents']]

        objects_list = []
        resp = self.s3.list_objects(Bucket=self.bucket, Prefix=prefix)
        objects_list.extend(resp_to_filelist(resp))
        logger.info("Found {} objects so far".format(len(objects_list)))
        while resp['IsTruncated']:
            last_key = objects_list[-1][0]
            resp = self.s3.list_objects(Bucket=self.bucket,
                                        Prefix=prefix,
                                        Marker=last_key)
            objects_list.extend(resp_to_filelist(resp))
            logger.info("Found {} objects so far".format(len(objects_list)))

        self.all_files.extend(objects_list)

    def add_file(self, key):
        resp = self.s3.get_object(Bucket=self.bucket, Key=key)
        self.all_files.append((key, resp['ContentLength']))
