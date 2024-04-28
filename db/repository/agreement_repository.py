from datetime import date

import ydb

from db.utils import execute_modifying_query


class AgreementRepository:
    SAVE_WITH_AUDIT_QUERY = """
        declare $agreement_id as Text;
        declare $buid as Text;
        declare $status as Text;
        declare $opening_date as Date;
        declare $auth_level as Text;
        declare $revision as Int64;
        insert into agreement(id, buid, status, opening_date, auth_level, revision) values
            ($agreement_id, $buid, $status, $opening_date, $auth_level, $revision);
        insert into agreement_audit(agreement_id, revision, buid, status, opening_date, auth_level) values
            ($agreement_id, $revision, $buid, $status, $opening_date, $auth_level);
    """

    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def insert_agreement(self, id, buid, status, auth_level):
        execute_modifying_query(
            pool=self.__ydb_pool,
            query=self.SAVE_WITH_AUDIT_QUERY,
            kwargs={
                "$agreement_id": id,
                "$buid": buid,
                "$status": status,
                "$opening_date": date.today(),
                "$auth_level": auth_level,
                "$revision": 0,
            })
