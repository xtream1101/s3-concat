# Python S3 Concat

[![PyPI](https://img.shields.io/pypi/v/s3-concat.svg)](https://pypi.python.org/pypi/s3-concat)
[![PyPI](https://img.shields.io/pypi/l/s3-concat.svg)](https://pypi.python.org/pypi/s3-concat)  


S3 Concat is used to concatenate many small files in an s3 bucket into fewer larger files.


## Install
`pip install s3-concat`


## Usage

### Command Line
`$ s3-concat -h`

### Import
```python
from s3_concat import S3Concat

bucket = 'YOUR_BUCKET_NAME'
path_to_concat = 'PATH_TO_FILES_TO_CONCAT'
concatenated_file = 'FILE_TO_SAVE_TO.json'
# Setting this to a size will always add a part number at the end of the file name
min_file_size = '50MB'  # ex: FILE_TO_SAVE_TO-1.json, FILE_TO_SAVE_TO-2.json, ...
# Setting this to None will concat all files into a single file
# min_file_size = None  ex: FILE_TO_SAVE_TO.json

# Init the job
job = S3Concat(bucket, concatenated_file, min_file_size,
               content_type='application/json',
              #  session=boto3.session.Session(),  # For custom aws session
              # s3_client_kwargs={}  # Use to pass arguments allowed by the s3 client: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
               )
# Add files, can call multiple times to add files from other directories
job.add_files(path_to_concat)
# Add a single file at a time
job.add_file('some/file_key.json')
# Only use small_parts_threads if you need to. See Advanced Usage section below.
job.concat(small_parts_threads=4, main_threads=2)
```

## Advanced Usage

Depending on your use case, you may want to use more threads then just 1.  

  - `main_threads` is the number of threads to use when uploading files to s3. This will help when there are a lot fo files that are already over the `min_file_size` that is set

  - `small_parts_threads` is only used when the files you are trying to concat are less then 5MB. These are spawned from _inside_ of the `main_threads`. Due to the limitations of the s3 multipart_upload api (see *Limitations* below) any files less then 5MB need to be download locally, concated together, then re uploaded. By setting this thread count it will download the parts in parallel for faster creation of the concatination process.  

The values set for these arguments depends on your use case and the system you are running this on.


## Limitations
This uses the multipart upload of s3 and its limits are https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
