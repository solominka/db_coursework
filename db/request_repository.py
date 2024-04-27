import datetime
import os
import ydb
from db.utils import execute_select_query


class RequestRepository:
    SELECT_BY_IDEMPOTENCY_TOKEN_QUERY = """
        declare $idempotency_token as Text;
        select * from request where idempotency_token = $idempotency_token; 
    """

    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def find_by_idempotency_token(self, idempotency_token):
        return execute_select_query(
            pool=self.__ydb_pool,
            query=self.SELECT_BY_IDEMPOTENCY_TOKEN_QUERY,
            kwargs={
                "$idempotency_token": idempotency_token
            })

    def save(self, idempotency_token, body, request_type):
        column_types = ydb.BulkUpsertColumns() \
            .add_column("idempotency_token", ydb.PrimitiveType.Utf8) \
            .add_column("body", ydb.PrimitiveType.Utf8) \
            .add_column("request_type", ydb.PrimitiveType.Utf8) \
            .add_column("created_at", ydb.PrimitiveType.Timestamp)

        kwargs = [{
            "idempotency_token": idempotency_token,
            "body": body,
            "request_type": request_type,
            "created_at": datetime.datetime.now(),
        }]

        self.__ydb_driver.table_client.bulk_upsert(
            os.getenv("YDB_DATABASE") + '/request',
            kwargs,
            column_types)
