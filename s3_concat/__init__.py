import re
import boto3
import logging
import argparse
from functools import partial
from multiprocessing import Pool

logger = logging.getLogger(__name__)
# S3 multi-part upload parts must be larger than 5mb
KB = 1024
MB = KB**2
GB = KB**3
TB = KB**4
MIN_S3_SIZE = 5 * MB


def _create_s3_client():
    session = boto3.session.Session()
    return session.client('s3')


def _chunk_by_size(file_list, min_file_size):
        """Split list by size of file

        Arguments:
            file_list {list} -- List of tuples as (<filename>, <file_size>)
            min_file_size {int} -- Min part file size in bytes

        Returns:
            list -- Each list of files is the min file size
        """
        grouped_list = []
        current_list = []
        current_size = 0
        current_index = 1
        for p in file_list:
            current_size += p[1]
            current_list.append(p)
            if current_size > min_file_size:
                grouped_list.append((current_index, current_list))
                current_list = []
                current_size = 0
                current_index += 1

        # Get anything left over
        if current_size != 0:
            grouped_list.append((current_index, current_list))

        return grouped_list


def _convert_to_bytes(value):
    """Convert the input value to bytes

    Arguments:
        value {string} -- Value and size of the input with no spaces

    Returns:
        float -- The value converted to bytes as a float

    Raises:
        ValueError -- if the input value is not a valid type to convert
    """
    value = value.strip()
    sizes = {'KB': 1024,
             'MB': 1024**2,
             'GB': 1024**3,
             'TB': 1024**4,
             }
    if value[-2:].upper() in sizes:
        return float(value[:-2].strip()) * sizes[value[-2:].upper()]
    elif re.match(r'^\d+(\.\d+)?$', value):
        return float(value)
    elif re.match(r'^\d+(\.\d+)?\s?B$', value):
        return float(value[:-1])
    else:
        raise ValueError("Value {} is not a valid size".format(value))


class MultipartUploadJob:

    def __init__(self, bucket, result_filepath, data_input, thread_count=1):
        # threading support comming soon
        self.bucket = bucket
        self.part_number, self.parts_list = data_input
        self.result_filepath = '{}-{}'.format(result_filepath, self.part_number)
        self.thread_count = thread_count  # Not yet used

        # s3 cannot be a class var because the Pool cannot pickle it
        s3 = _create_s3_client()
        if len(self.parts_list) > 0:
            self.upload_id = self._start_multipart_upload(s3)
            parts_mapping = self._assemble_parts(s3)
            self._complete_concatenation(s3, parts_mapping)

        elif len(self.parts_list) == 1:
            # can perform a simple S3 copy since there is just a single file
            resp = s3.copy_object(Bucket=self.bucket,
                                  CopySource="{}/{}".format(self.bucket, self.parts_list[0][0]),
                                  Key=self.result_filepath)
            logger.warning("Copied single file to {} and got response {}"
                           .format(self.result_filepath, resp))

    def _start_multipart_upload(self, s3):
        resp = s3.create_multipart_upload(Bucket=self.bucket,
                                          Key=self.result_filepath)
        logger.warning("Started multipart upload for {}, and got response: {}"
                       .format(self.result_filepath, resp))
        return resp['UploadId']

    def _assemble_parts(self, s3):
        # TODO: Thread the loops in this function
        parts_mapping = []
        part_num = 0

        s3_parts = ["{}/{}".format(self.bucket, p[0]) for p in self.parts_list if p[1] > MIN_S3_SIZE]
        local_parts = [p for p in self.parts_list if p[1] <= MIN_S3_SIZE]

        # assemble parts large enough for direct S3 copy
        for part_num, source_part in enumerate(s3_parts, 1):  # part numbers are 1 indexed
            resp = s3.upload_part_copy(Bucket=self.bucket,
                                       Key=self.result_filepath,
                                       PartNumber=part_num,
                                       UploadId=self.upload_id,
                                       CopySource=source_part)
            logger.warning("Setup S3 part #{}, with path: {}, and got response: {}"
                           .format(part_num, source_part, resp))
            parts_mapping.append({'ETag': resp['CopyPartResult']['ETag'][1:-1], 'PartNumber': part_num})

        # assemble parts too small for direct S3 copy by downloading them locally,
        # combining them, and then reuploading them as the last part of the
        # multi-part upload (which is not constrained to the 5mb limit)

        # Concat the small_parts into the minium size then upload so not much is ever kept in memory
        for local_parts_part in _chunk_by_size(local_parts, MIN_S3_SIZE * 2):
            small_parts = []
            for source_part in local_parts_part[1]:
                # Keep it all in memory
                foo = s3.get_object(Bucket=self.bucket,
                                    Key=source_part[0])['Body'].read().decode('utf-8')
                small_parts.append(foo)
                foo = None  # cleanup

            if len(small_parts) > 0:
                part_num += 1
                last_part = ''.join(small_parts)
                small_part_count = len(small_parts)
                small_parts = None  # cleanup
                resp = s3.upload_part(Bucket=self.bucket,
                                      Key=self.result_filepath,
                                      PartNumber=part_num,
                                      UploadId=self.upload_id,
                                      Body=last_part)
                logger.warning("Setup local part #{} from {} small files, and got response: {}"
                               .format(part_num, small_part_count, resp))
                parts_mapping.append({'ETag': resp['ETag'][1:-1], 'PartNumber': part_num})

            last_part = None  # cleanup

        return parts_mapping

    def _complete_concatenation(self, s3, parts_mapping):
        if len(parts_mapping) == 0:
            s3.abort_multipart_upload(Bucket=self.bucket,
                                      Key=self.result_filepath,
                                      UploadId=self.upload_id)
            logger.warning("Aborted concatenation for file {}, with upload id #{} due to empty parts mapping"
                           .format(self.result_filepath, self.upload_id))
        else:
            s3.complete_multipart_upload(Bucket=self.bucket,
                                         Key=self.result_filepath,
                                         UploadId=self.upload_id,
                                         MultipartUpload={'Parts': parts_mapping})
            logger.warning("Finished concatenation for file {}, with upload id #{}, and parts mapping: {}"
                           .format(self.result_filepath, self.upload_id, parts_mapping))


class S3Concat:

    def __init__(self, bucket, key, min_file_size):
        self.bucket = bucket
        self.key = key
        self.min_file_size = _convert_to_bytes(min_file_size)
        self.all_files = []
        self.s3 = _create_s3_client()

    def concat(self, process_count=4):

        grouped_parts_list = _chunk_by_size(self.all_files, self.min_file_size)
        logger.warning("Created {} concatenation groups".format(len(grouped_parts_list)))

        pool = Pool(processes=process_count)
        func = partial(MultipartUploadJob,
                       self.bucket,
                       self.key,
                       thread_count=1,)
        pool.map(func, grouped_parts_list)

    def add_files(self, prefix):

        def resp_to_filelist(resp):
            return [(x['Key'], x['Size']) for x in resp['Contents']]

        objects_list = []
        resp = self.s3.list_objects(Bucket=self.bucket, Prefix=prefix)
        objects_list.extend(resp_to_filelist(resp))
        logger.warning("Found {} objects so far".format(len(objects_list)))
        while resp['IsTruncated']:
            last_key = objects_list[-1][0]
            resp = self.s3.list_objects(Bucket=self.bucket,
                                        Prefix=self.key,
                                        Marker=last_key)
            objects_list.extend(resp_to_filelist(resp))
            logger.warning("Found {} objects so far".format(len(objects_list)))

        self.all_files.extend(objects_list)


def cli():
    """Command line tool to convert data file into other formats
    Notes:
        Not using pathlib because trying to keep this as compatible as posiable with other versions
    """
    parser = argparse.ArgumentParser(description='Convert data files')
    parser.add_argument("--bucket",
                        help="base bucket to use",
                        required=True,
                        )
    parser.add_argument("--folder",
                        help="folder whose contents should be combined",
                        required=True,
                        )
    parser.add_argument("--output",
                        help=("output location for resulting merged files,"
                              " relative to the specified base bucket"),
                        required=True,
                        )
    parser.add_argument("--filesize",
                        help=("Min filesize of the concatenated files"
                              " in [B,KB,MB,GB,TB]. e.x. 5.2GB"),
                        required=True,
                        )
    parser.add_argument("--processes",
                        type=int,
                        help="Number of processes to run. (Default: 4)",
                        default=4)
    args = parser.parse_args()

    job = S3Concat(args.bucket, args.output, args.filesize)
    job.add_files(args.folder)
    job.concat(args.processes)

