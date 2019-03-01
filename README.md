# Python S3 Concat

S3 Concat is used to concatenate many small files in an s3 bucket into fewer larger files.


## Install
`pip install s3-concat`


## Usage

### Command Line
`$ s3-concat -h`

### Import
```python
bucket = 'YOUR_BUCKET_NAME'
out_path = 'FILE_TO_SAVE_TO'
min_file_size = '50MB'
path_to_concat = 'PATH_TO_FILES_TO_CONCAT'

# Init the job
job = S3Concat(bucket, out_path, min_file_size, content_type='application/json')
# Add files, can call multiple times to add files from other directories
job.add_files(path_to_concat)
# Run the job using 6 processes. This is BLOCKING and is using a multiprocessing pool
job.concat(6)
```

## Limitations
This uses the multipart upload of s3 and its limits are https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
