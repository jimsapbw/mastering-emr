import boto3
s3_client = boto3.client('s3')
buckets = s3_client.list_buckets()['Buckets']
for bucket in buckets:
    print(bucket['Name'])