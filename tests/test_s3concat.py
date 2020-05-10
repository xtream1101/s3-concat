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

    tar = S3Concat('my-bucket', 'all_data.json', '10MB', session=session)
    tar.add_file('some_folder/thing1.json')

    assert tar.all_files == [('some_folder/thing1.json', 29)]
