import datetime

import ydb

from db.utils import execute_modifying_query


class LockRepository:
    ACQUIRE_LOCK_QUERY = """
        declare $buid as Text;
        declare $agreement_id as Text;
        declare $acquired_at as Timestamp;
        
        insert into lock (buid, agreement_id, acquired_at) values ($buid, $agreement_id, $acquired_at);
    """

    RELEASE_LOCK_QUERY = """
            declare $buid as Text;
            declare $agreement_id as Text;
            
            delete from lock where buid = $buid and agreement_id = $agreement_id;
    """

    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def acquire_lock(self, buid='', agreement_id=''):
        execute_modifying_query(
            pool=self.__ydb_pool,
            query=self.ACQUIRE_LOCK_QUERY,
            kwargs={
                "$buid": buid,
                "$agreement_id": agreement_id,
                "$acquired_at": datetime.datetime.now(),
            })

    def release_lock(self, buid='', agreement_id=''):
        execute_modifying_query(
            pool=self.__ydb_pool,
            query=self.RELEASE_LOCK_QUERY,
            kwargs={
                "$buid": buid,
                "$agreement_id": agreement_id,
            })
