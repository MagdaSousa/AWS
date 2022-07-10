# boto 3 https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html

import boto3
import os
import pandas


# verificar os buckets criados no s3


class BucketAWS:
    def __init__(self):
        self.s3_resource = boto3.resource('s3')
        self.client = boto3.client('s3')
        self.name_bucket = 'igti-bucket-datalake'
        self.dest_folder = 'data'
        self.folder_to_upload_path = '/S3/file_to_upload/teste.json'

    def buckets_name(self):
        for bucket in self.s3.buckets.all():
            print(bucket.name)

    # criando client para interagir com o AWS S3

    def upload_file_to_bucket(self):
        # Upload a new file
        data = open(self.folder_to_upload_path, 'rb')
        self.s3_resource.Bucket(self.name_bucket).put_object(Key=f'{self.dest_folder}/teste.json', Body=data)

    def download_file_to_bucket(self):
        #fazendo o download de um arquivo que está no bucket no S3
        response = self.s3_resource.meta.client.download_file(self.name_bucket, 'hello.txt', 'hello.txt')
        return print(response)

    def delete_file_in_bucket(self):
        # deletando um objeto de um bucket
        response = self.client.delete_object(
            Bucket=self.name_bucket,
            Key='teste.json',
        )
        # response example:
        """ {'ResponseMetadata': {'RequestId': '***************',
                                  'HostId': '***************',
                                  'HTTPStatusCode': 204,
                                  'HTTPHeaders': {
                                                    'x-amz-id-2': '***************',
                                                    'x-amz-request-id': '***************',
                                                    'date': 'Sun, 10 Jul 2022 22:59:34 GMT',
                                                    'server': 'AmazonS3'},
                                                    'RetryAttempts': 0}
                                                }"""

        return print(response)


if __name__ == '__main__':
    aws = BucketAWS()
    # aws.upload_file_to_bucket()
    # aws.delete_file_in_bucket()
    aws.download_file_to_bucket()
