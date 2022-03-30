import boto3


#Creating Session With Boto3.
session = boto3.Session(
aws_access_key_id='AKIA6FLGTZADUI6KEFX7',
aws_secret_access_key='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'
)

#Creating S3 Resource From the Session.
s3 = session.resource('s3')

txt_data = b'This is the content of the file uploaded from python boto3 asdfasdf'

object = s3.Object('sfiec-jammesson', 'file_name.txt')

result = object.put(Body=txt_data)