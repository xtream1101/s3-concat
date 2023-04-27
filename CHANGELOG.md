# Change log

### 0.2.4
- Any file that is larger then the min_file_size will not get combined with a smaller file anymore
    - Issue was if a small file was to small, and the next file was larger then the min size it would still combine them
- Added threads the whole process, that way even the large files will be working in threads


### 0.2.3
- Fixed memory leak as noted in [issue #11](https://github.com/xtream1101/s3-concat/issues/11)

### 0.2.2
- Support quoted and un-quoted etag strings

### 0.2.1
- Allow binary files to be concat'd. NOTE: Will not work for all file types, only the ones that support it, like gzip.
- Added Changelog
