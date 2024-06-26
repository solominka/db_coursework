import datetime
import ydb
from db.utils import execute_reading_query, execute_modifying_query
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
        declare $success as Bool;
        
        update request set 
            created_entity_id = $created_entity_id,
            success = $success
        where idempotency_token = $idempotency_token; 
    """

    SAVE_REQUEST_QUERY = """
        declare $idempotency_token as Text;
        declare $body as Text;
        declare $request_type as Text;
        declare $created_at as Timestamp;
        insert into request(idempotency_token, body, request_type, created_at, success) values
            ($idempotency_token, $body, $request_type, $created_at, False);
    """

    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def save_result(self, idempotency_token, created_entity_id='', success=True):
        execute_modifying_query(
            pool=self.__ydb_pool,
            query=self.UPDATE_CREATED_ENTITY_ID_QUERY,
            kwargs={
                "$idempotency_token": idempotency_token,
                '$created_entity_id': created_entity_id,
                '$success': success,
            })

    def save_or_get(self, idempotency_token, body, request_type):
        try:
            self.save(idempotency_token=idempotency_token, body=body, request_type=request_type)
        except ydb.issues.PreconditionFailed:
            existing_request = self.find_by_idempotency_token(idempotency_token=idempotency_token,
                                                              request_type=request_type)
            if len(existing_request) != 0:
                if existing_request[0]['body'] == body:
                    if bool(existing_request[0]['success']):
                        return existing_request[0]
                    return
                else:
                    raise IdempotencyViolationException(message="idempotency violation on {}".format(request_type))

    def find_by_idempotency_token(self, idempotency_token, request_type):
        return execute_reading_query(
            pool=self.__ydb_pool,
            query=self.SELECT_BY_IDEMPOTENCY_TOKEN_QUERY,
            kwargs={
                "$idempotency_token": idempotency_token,
                "$request_type": request_type
            })

    def save(self, idempotency_token, body, request_type):
        execute_modifying_query(
            pool=self.__ydb_pool,
            query=self.SAVE_REQUEST_QUERY,
            kwargs={
                "$idempotency_token": idempotency_token,
                "$body": body,
                "$request_type": request_type,
                "$created_at": datetime.datetime.now(),
            })
