import boto3
from s3_concat import S3Concat
from moto import mock_s3


###
# add_file
###
@mock_s3
def test_add_file():
    session = boto3.session.Session()
    s3 = session.client('s3')
    # Need to create the bucket since this is in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket='my-bucket')
    s3.put_object(
        Bucket='my-bucket',
        Key='some_folder/thing1.json',
        Body=b'{"foo": "Test File Contents"}',
    )

    concat = S3Concat('my-bucket', 'all_data.json', '10MB', session=session)
    concat.add_file('some_folder/thing1.json')

    assert concat.all_files == [('some_folder/thing1.json', 29)]


@mock_s3
def test_concat_text_file():
    session = boto3.session.Session()
    s3 = session.client('s3')
    # Need to create the bucket since this is in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket='my-bucket')
    s3.put_object(
        Bucket='my-bucket',
        Key='some_folder/thing1.json',
        Body=b'Thing1\n',
    )
    s3.put_object(
        Bucket='my-bucket',
        Key='some_folder/thing2.json',
        Body=b'Thing2\n',
    )

    concat = S3Concat('my-bucket', 'all_things.json', None, session=session)
    concat.add_files('some_folder')
    concat.concat()

    concat_output = s3.get_object(
        Bucket='my-bucket',
        Key='all_things.json'
    )['Body'].read().decode('utf-8')

    assert concat_output == 'Thing1\nThing2\n'


@mock_s3
def test_concat_gzip_content():
    """Create 2 gzip files, then use s3concat to create a single gzip file
    To test, un-compress and read contents of the concat'd file
    """
    import gzip
    import tempfile
    session = boto3.session.Session()
    s3 = session.client('s3')
    # Need to create the bucket since this is in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket='my-bucket')

    file1 = tempfile.NamedTemporaryFile()
    with gzip.open(file1.name, 'wb') as f:
        f.write(b"file 1 contents\n")
    s3.upload_file(file1.name, 'my-bucket', 'some_folder/thing1.gz')

    file2 = tempfile.NamedTemporaryFile()
    with gzip.open(file2.name, 'wb') as f:
        f.write(b"file 2 contents\n")
    s3.upload_file(file2.name, 'my-bucket', 'some_folder/thing2.gz')

    concat = S3Concat('my-bucket', 'all_data.gz', None, session=session)
    concat.add_files('some_folder')
    concat.concat()

    all_data_file = tempfile.NamedTemporaryFile()

    s3.download_file('my-bucket', 'all_data.gz', all_data_file.name)

    with gzip.open(all_data_file.name, 'rb') as f:
        content_output = f.read()

    assert content_output == b'file 1 contents\nfile 2 contents\n'
