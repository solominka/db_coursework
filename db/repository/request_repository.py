import datetime
import os
import ydb
from db.utils import execute_select_query, execute_update_query
from exceptions import IdempotencyViolationException


class RequestRepository:
    SELECT_BY_IDEMPOTENCY_TOKEN_QUERY = """
        declare $idempotency_token as Text;
        declare $request_type as Text;
        select * from request where idempotency_token = $idempotency_token and request_type = $request_type; 
    """

    UPDATE_CREATED_ENTITY_ID_QUERY = """
            declare $idempotency_token as Text;
            declare $created_entity_id as Text;
            update request set created_entity_id = $created_entity_id where idempotency_token = $idempotency_token; 
        """

    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def save_created_entity_id(self, idempotency_token, created_entity_id):
        execute_update_query(
            pool=self.__ydb_pool,
            query=self.UPDATE_CREATED_ENTITY_ID_QUERY,
            kwargs={
                "$idempotency_token": idempotency_token,
                '$created_entity_id': created_entity_id,
            })

    def save_or_get(self, idempotency_token, body, request_type):
        existing_request = self.find_by_idempotency_token(idempotency_token, request_type)
        if len(existing_request) != 0:
            if existing_request[0]['body'] == body:
                return existing_request[0]
            else:
                raise IdempotencyViolationException(message="idempotency violation on {}".format(request_type))
        else:
            self.save(idempotency_token=idempotency_token, body=body, request_type=request_type)

    def find_by_idempotency_token(self, idempotency_token, request_type):
        return execute_select_query(
            pool=self.__ydb_pool,
            query=self.SELECT_BY_IDEMPOTENCY_TOKEN_QUERY,
            kwargs={
                "$idempotency_token": idempotency_token,
                "$request_type": request_type
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
