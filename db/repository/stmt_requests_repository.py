import datetime
from uuid import uuid4

import ydb

from db.utils import execute_modifying_query


class StmtRequestRepository:
    SAVE_REQUEST_QUERY = """
        declare $id as Text;
        declare $buid as Text;
        declare $txn_id as Text;
        declare $success as Bool;
        declare $created_at as Timestamp;
        declare $fail_reason as Text;
        
        insert into stmt_request(id, buid, txn_id, success, created_at, fail_reason) values
            ($id, $buid, $txn_id, $success, $created_at, $fail_reason);
    """

    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def save_request(self, txn_id, buid, success, fail_reason=''):
        execute_modifying_query(
            pool=self.__ydb_pool,
            query=self.SAVE_REQUEST_QUERY,
            kwargs={
                '$id': str(uuid4()),
                '$buid': buid,
                "$txn_id": txn_id,
                '$success': success,
                '$created_at': datetime.datetime.now(),
                '$fail_reason': fail_reason
            })
