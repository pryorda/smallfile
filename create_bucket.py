#!/usr/bin/python
import boto
import boto.s3.connection
import os
from boto.s3.key import Key

STORAGEAPI = os.getenv('STORAGEAPI')
access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET = os.getenv('BUCKET')

# Create Connection
conn = boto.connect_s3(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        host='localhost',
        is_secure=False,               # uncomment if you are not using ssl
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        )

# Create Bucket
bucket = conn.create_bucket(str(BUCKET).lower())

# List Bucket
for bucket in conn.get_all_buckets():
        print "{name}\t{created}".format(
                name=bucket.name,
                created=bucket.creation_date,
        )

# Create Folder
filename = "02e5fba3-b2ef-4955-b5b8-47acbd4a60fe"
sFName = filename.replace('-', '')
folder_structure = sFName[0:2] + "/" + sFName[2:5] + "/" + sFName[5:8] \
  + "/" + sFName[8:11] + "/" + sFName[11:14] + "/" + sFName[14:17] + "/"


# Put File
put_key = Key(bucket)
put_key.key = folder_structure + filename
put_key.set_contents_from_filename('02e5fba3-b2ef-4955-b5b8-47acbd4a60fe')

# Get File
get_key = bucket.get_key(str(folder_structure + filename))
get_key.get_contents_to_filename('my_s3_testfile')

# List Keys
for x in bucket.list():
    print x.name
