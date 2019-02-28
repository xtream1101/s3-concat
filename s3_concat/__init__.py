import os
import boto3
import logging
import argparse
from functools import partial
from multiprocessing import Pool

logger = logging.getLogger(__name__)
BUCKET = ''
# S3 multi-part upload parts must be larger than 5mb
MIN_S3_SIZE = 5242880


def run_concatenation(folder_to_concatenate, result_filepath, file_suffix, max_filesize, processes):
    s3 = new_s3_client()
    parts_list = collect_parts(s3, folder_to_concatenate, file_suffix)
    logger.warning("Found {} parts to concatenate in {}/{}".format(len(parts_list), BUCKET, folder_to_concatenate))

    # limits https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
    grouped_parts_list = chunk_by_size(parts_list, max_filesize)
    logger.warning("Created {} concatenation groups".format(len(grouped_parts_list)))
    pool = Pool(processes=processes)
    func = partial(run_single_concatenation, result_filepath, max_filesize)
    pool.map(func, grouped_parts_list)


def run_single_concatenation(result_filepath, max_filesize, data_input):
    s3 = new_s3_client()
    idx, parts_list = data_input
    result_filepath = '{}-{}'.format(result_filepath, idx)

    if len(parts_list) > 1:
        # perform multi-part upload
        upload_id = initiate_concatenation(s3, result_filepath)
        parts_mapping = assemble_parts_to_concatenate(s3, result_filepath, upload_id, parts_list, max_filesize)
        complete_concatenation(s3, result_filepath, upload_id, parts_mapping)
    elif len(parts_list) == 1:
        # can perform a simple S3 copy since there is just a single file
        resp = s3.copy_object(Bucket=BUCKET, CopySource="{}/{}".format(BUCKET, parts_list[0][0]), Key=result_filepath)
        logger.warning("Copied single file to {} and got response {}".format(result_filepath, resp))
    else:
        logger.warning("No files to concatenate for {}".format(result_filepath))
        pass


def initiate_concatenation(s3, result_filename):
    # performing the concatenation in S3 requires creating a multi-part upload
    # and then referencing the S3 files we wish to concatenate as "parts" of that upload
    resp = s3.create_multipart_upload(Bucket=BUCKET, Key=result_filename)
    logger.warning("Initiated concatenation attempt for {}, and got response: {}".format(result_filename, resp))
    return resp['UploadId']

def assemble_parts_to_concatenate(s3, result_filename, upload_id, parts_list, max_filesize):
    parts_mapping = []
    part_num = 0

    s3_parts = ["{}/{}".format(BUCKET, p[0]) for p in parts_list if p[1] > MIN_S3_SIZE]
    local_parts = [p for p in parts_list if p[1] <= MIN_S3_SIZE]

    # assemble parts large enough for direct S3 copy
    for part_num, source_part in enumerate(s3_parts, 1): # part numbers are 1 indexed
        resp = s3.upload_part_copy(Bucket=BUCKET,
                                   Key=result_filename,
                                   PartNumber=part_num,
                                   UploadId=upload_id,
                                   CopySource=source_part)
        logger.warning("Setup S3 part #{}, with path: {}, and got response: {}".format(part_num, source_part, resp))
        parts_mapping.append({'ETag': resp['CopyPartResult']['ETag'][1:-1], 'PartNumber': part_num})

    # assemble parts too small for direct S3 copy by downloading them locally,
    # combining them, and then reuploading them as the last part of the
    # multi-part upload (which is not constrained to the 5mb limit)

    # Concat the small_parts into the minium size then upload so not much is ever kept in memory
    for local_parts_part in chunk_by_size(local_parts, min(MIN_S3_SIZE * 2, max_filesize)):
        small_parts = []
        for source_part in local_parts_part[1]:
            # Keep it all in memory
            foo = s3.get_object(Bucket=BUCKET, Key=source_part[0])['Body'].read().decode('utf-8')
            small_parts.append(foo)
            foo = None  # cleanup

        if len(small_parts) > 0:
            part_num += 1
            last_part = ''.join(small_parts)
            small_part_count = len(small_parts)
            small_parts = None  # cleanup
            resp = s3.upload_part(Bucket=BUCKET, Key=result_filename, PartNumber=part_num, UploadId=upload_id, Body=last_part)
            logger.warning("Setup local part #{} from {} small files, and got response: {}".format(part_num, small_part_count, resp))
            parts_mapping.append({'ETag': resp['ETag'][1:-1], 'PartNumber': part_num})

        last_part = None  # cleanup

    return parts_mapping

def complete_concatenation(s3, result_filename, upload_id, parts_mapping):
    if len(parts_mapping) == 0:
        resp = s3.abort_multipart_upload(Bucket=BUCKET, Key=result_filename, UploadId=upload_id)
        logger.warning("Aborted concatenation for file {}, with upload id #{} due to empty parts mapping".format(result_filename, upload_id))
    else:
        resp = s3.complete_multipart_upload(Bucket=BUCKET, Key=result_filename, UploadId=upload_id, MultipartUpload={'Parts': parts_mapping})
        logger.warning("Finished concatenation for file {}, with upload id #{}, and parts mapping: {}".format(result_filename, upload_id, parts_mapping))


def chunk_by_size(parts_list, max_filesize):
    grouped_list = []
    current_list = []
    current_size = 0
    current_index = 1
    for p in parts_list:
        current_size += p[1]
        current_list.append(p)
        if current_size > max_filesize:
            grouped_list.append((current_index, current_list))
            current_list = []
            current_size = 0
            current_index += 1

    # Get anything left over
    if current_size != 0:
        grouped_list.append((current_index, current_list))

    return grouped_list

def new_s3_client():
    # initialize an S3 client with a private session so that multithreading
    # doesn't cause issues with the client's internal state
    session = boto3.session.Session()
    return session.client('s3')

def collect_parts(s3, folder, suffix):
    if suffix is None:
        suffix = ''
    return list(filter(lambda x: x[0].endswith(suffix), _list_all_objects_with_size(s3, folder)))

def _list_all_objects_with_size(s3, folder):

    def resp_to_filelist(resp):
        return [(x['Key'], x['Size']) for x in resp['Contents']]

    objects_list = []
    resp = s3.list_objects(Bucket=BUCKET, Prefix=folder)
    objects_list.extend(resp_to_filelist(resp))
    while resp['IsTruncated']:
        # if there are more entries than can be returned in one request, the key
        # of the last entry returned acts as a pagination value for the next request
        logger.warning("Found {} objects so far".format(len(objects_list)))
        last_key = objects_list[-1][0]
        resp = s3.list_objects(Bucket=BUCKET, Prefix=folder, Marker=last_key)
        objects_list.extend(resp_to_filelist(resp))

    return objects_list


def cli():
    global BUCKET
    """Command line tool to convert data file into other formats
    Notes:
        Not using pathlib because trying to keep this as compatible as posiable with other versions
    """
    parser = argparse.ArgumentParser(description='Convert data files')
    parser.add_argument("--bucket", help="base bucket to use")
    parser.add_argument("--folder", help="folder whose contents should be combined")
    parser.add_argument("--output", help="output location for resulting merged files, relative to the specified base bucket")
    parser.add_argument("--suffix", help="suffix of files to include in the combination")
    parser.add_argument("--filesize", type=int, help="max filesize of the concatenated files in bytes")
    parser.add_argument("--processes", type=int, help="Number of parallel processes to run. (Default: 4)", default=4)
    args = parser.parse_args()

    try:
        BUCKET = args.bucket
        run_concatenation(args.folder, args.output, args.suffix, args.filesize, args.processes)
    except Exception:
        logger.exception("Something went wrong")
