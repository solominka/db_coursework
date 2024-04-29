import boto3


class TransactionStmtRepository:
    def __init__(self):
        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net'
        )
        self.__s3 = s3

    def upload_file(self, file_path, key):
        self.__s3.upload_file(file_path, 'stmt', key)

    def get_file(self, key):
        self.__s3.download_file('stmt', key, '/Users/introvertess/Downloads/{}'.format(key))
