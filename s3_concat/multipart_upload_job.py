import logging
from .utils import _create_s3_client, _threads, _chunk_by_size, MIN_S3_SIZE

logger = logging.getLogger(__name__)


class MultipartUploadJob:

    def __init__(self, bucket, result_filepath, data_input,
                 small_parts_threads=1,
                 content_type='application/octet-stream'):
        # threading support comming soon
        self.bucket = bucket
        self.part_number, self.parts_list = data_input
        self.content_type = content_type
        self.small_parts_threads = small_parts_threads

        if '.' in result_filepath.split('/')[-1]:
            # If there is a file extention, put the part number before it
            path_parts = result_filepath.rsplit('.', 1)
            self.result_filepath = '{}-{}.{}'.format(path_parts[0],
                                                     self.part_number,
                                                     path_parts[1])
        else:
            self.result_filepath = '{}-{}'.format(result_filepath,
                                                  self.part_number)

        # s3 cannot be a class var because the Pool cannot pickle it
        s3 = _create_s3_client()
        if len(self.parts_list) > 0:
            self.upload_id = self._start_multipart_upload(s3)
            parts_mapping = self._assemble_parts(s3)
            self._complete_concatenation(s3, parts_mapping)

        elif len(self.parts_list) == 1:
            # can perform a simple S3 copy since there is just a single file
            source_file = "{}/{}".format(self.bucket, self.parts_list[0][0])
            resp = s3.copy_object(Bucket=self.bucket,
                                  CopySource=source_file,
                                  Key=self.result_filepath)
            msg = "Copied single file to {}".format(self.result_filepath)
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.debug("{}, got response: {}".format(msg, resp))
            else:
                logger.info(msg)

    def _start_multipart_upload(self, s3):
        resp = s3.create_multipart_upload(Bucket=self.bucket,
                                          Key=self.result_filepath,
                                          ContentType=self.content_type)
        msg = "Started multipart upload for {}".format(self.result_filepath)
        if logger.getEffectiveLevel() == logging.DEBUG:
            logger.debug("{}, got response: {}".format(msg, resp))
        else:
            logger.info(msg)
        return resp['UploadId']

    def _assemble_parts(self, s3):
        # TODO: Thread the loops in this function
        parts_mapping = []
        part_num = 0

        s3_parts = ["{}/{}".format(self.bucket, p[0])
                    for p in self.parts_list if p[1] > MIN_S3_SIZE]

        local_parts = [p for p in self.parts_list if p[1] <= MIN_S3_SIZE]

        # assemble parts large enough for direct S3 copy
        for part_num, source_part in enumerate(s3_parts, 1):
            resp = s3.upload_part_copy(Bucket=self.bucket,
                                       Key=self.result_filepath,
                                       PartNumber=part_num,
                                       UploadId=self.upload_id,
                                       CopySource=source_part)
            msg = "Setup S3 part #{}, with path: {}".format(part_num,
                                                            source_part)
            if logger.getEffectiveLevel() == logging.DEBUG:
                logger.debug("{}, got response: {}".format(msg, resp))
            else:
                logger.info(msg)

            parts_mapping.append({'ETag': resp['CopyPartResult']['ETag'][1:-1],
                                  'PartNumber': part_num})

        # assemble parts too small for direct S3 copy by downloading them,
        # combining them, and then reuploading them as the last part of the
        # multi-part upload (which is not constrained to the 5mb limit)

        # Concat the small_parts into the minium size then upload
        # this way not to much data is kept in memory
        def get_small_parts(data):
            part_num, part = data
            small_part_count = len(part[1])

            logger.info("Start sub-part #{} from {} files"
                        .format(part_num, small_part_count))

            small_parts = []
            for p in part[1]:
                small_parts.append(
                    s3.get_object(Bucket=self.bucket,
                                  Key=p[0]
                                  )['Body'].read().decode('utf-8')
                )

            if len(small_parts) > 0:
                last_part = ''.join(small_parts)

                small_parts = None  # cleanup
                resp = s3.upload_part(Bucket=self.bucket,
                                      Key=self.result_filepath,
                                      PartNumber=part_num,
                                      UploadId=self.upload_id,
                                      Body=last_part)
                msg = "Finish sub-part #{} from {} files"\
                      .format(part_num, small_part_count)
                if logger.getEffectiveLevel() == logging.DEBUG:
                    logger.debug("{}, got response: {}".format(msg, resp))
                else:
                    logger.info(msg)

                last_part = None

                return {'ETag': resp['ETag'][1:-1],
                        'PartNumber': part_num}
            return {}

        data_to_thread = []
        for idx, data in enumerate(_chunk_by_size(local_parts,
                                                  MIN_S3_SIZE * 2),
                                   start=1):
            data_to_thread.append([part_num + idx, data])

        parts_mapping.extend(
            _threads(self.small_parts_threads,
                     data_to_thread,
                     get_small_parts)
        )

        # Sort part mapping by part number
        return sorted(parts_mapping, key=lambda i: i['PartNumber'])

    def _complete_concatenation(self, s3, parts_mapping):
        if len(parts_mapping) == 0:
            s3.abort_multipart_upload(Bucket=self.bucket,
                                      Key=self.result_filepath,
                                      UploadId=self.upload_id)
            warn_msg = ("Aborted concatenation for file {}, with upload"
                        " id #{} due to empty parts mapping")
            logger.error(warn_msg.format(self.result_filepath,
                                         self.upload_id))
        else:
            parts = {'Parts': parts_mapping}
            s3.complete_multipart_upload(Bucket=self.bucket,
                                         Key=self.result_filepath,
                                         UploadId=self.upload_id,
                                         MultipartUpload=parts)
            warn_msg = ("Finished concatenation for file {},"
                        " with upload id #{}")
            logger.info(warn_msg.format(self.result_filepath,
                                        self.upload_id))
