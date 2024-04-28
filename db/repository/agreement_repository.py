from datetime import date

import ydb

from db.utils import execute_modifying_query, execute_reading_query


class AgreementRepository:
    SAVE_WITH_AUDIT_QUERY = """
        declare $agreement_id as Text;
        declare $product as Text;
        declare $buid as Text;
        declare $status as Text;
        declare $opening_date as Date;
        declare $auth_level as Text;
        declare $revision as Int64;
        insert into agreement(id, buid, product, status, opening_date, auth_level, revision) values
            ($agreement_id, $buid, $product, $status, $opening_date, $auth_level, $revision);
        insert into agreement_audit(agreement_id, revision, buid, product, status, opening_date, auth_level) values
            ($agreement_id, $revision, $buid, $product, $status, $opening_date, $auth_level);
    """

    CLOSE_WITH_AUDIT_QUERY = """
            declare $agreement_id as Text;
            declare $status as Text;
            declare $closing_date as Date;
            
            update agreement set
                status = $status,
                closing_date = $closing_date,
                revision = revision + 1
            where id = $agreement_id;
            
            insert into agreement_audit
                select 
                    id as agreement_id,
                    revision,
                    buid,
                    product,
                    status,
                    opening_date,
                    closing_date,
                    auth_level
                from agreement where 
                    id = $agreement_id;
        """

    UPGRADE_WITH_AUDIT_QUERY = """
                declare $buid as Text;
                declare $new_auth_level as Text;

                update agreement set
                    auth_level = $new_auth_level,
                    revision = revision + 1
                where buid = $buid and status = 'OPEN' and product = 'current_account';

                insert into agreement_audit
                    select 
                        id as agreement_id,
                        revision,
                        buid,
                        product,
                        status,
                        opening_date,
                        closing_date,
                        auth_level
                    from agreement where 
                        buid = $buid and status = 'OPEN';
            """

    SELECT_BY_ID_QUERY = """
        declare $id as Text;
        select * from agreement where id = $id;
    """

    SELECT_BY_BUID_QUERY = """
            declare $buid as Text;
            declare $status as Text;
            select * from agreement where buid = $buid and status = $status;
        """

    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def insert_agreement(self, id, buid, product, status, auth_level):
        execute_modifying_query(
            pool=self.__ydb_pool,
            query=self.SAVE_WITH_AUDIT_QUERY,
            kwargs={
                "$agreement_id": id,
                "$buid": buid,
                "$product": product,
                "$status": status,
                "$opening_date": date.today(),
                "$auth_level": auth_level,
                "$revision": 0,
            })

    def close_agreement(self, id):
        execute_modifying_query(
            pool=self.__ydb_pool,
            query=self.CLOSE_WITH_AUDIT_QUERY,
            kwargs={
                "$agreement_id": id,
                "$status": "CLOSED",
                "$closing_date": date.today(),
            })

    def find_by_id(self, id):
        return execute_reading_query(
            pool=self.__ydb_pool,
            query=self.SELECT_BY_ID_QUERY,
            kwargs={
                "$id": id,
            }
        )

    def find_by_buid(self, buid, status='OPEN'):
        return execute_reading_query(
            pool=self.__ydb_pool,
            query=self.SELECT_BY_BUID_QUERY,
            kwargs={
                "$buid": buid,
                "$status": status,
            }
        )

    def upgrade_current_account(self, buid, auth_level):
        execute_modifying_query(
            pool=self.__ydb_pool,
            query=self.UPGRADE_WITH_AUDIT_QUERY,
            kwargs={
                "$buid": buid,
                "$new_auth_level": auth_level,
            })
