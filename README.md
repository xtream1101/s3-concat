# Python S3 Concat

S3 Concat is used to concatenate many small files in an s3 bucket into fewer larger files.


## Install
_Comming soon:_ ~`pip install s3-concat`~


## Usage
This installs a cli called `s3-concat`. Find out how to use it by running `s3-concat -h`.  
It can also be imported and used in any other python scripts by foing `import s3_concat`.  
More docs to follow on that.


## Limitations
This uses the multipart upload of s3 and its limits are https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html  
TODO: give details on how it deals with files smaller then 5MB because it is handled differently.
