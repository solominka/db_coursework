import os
from datetime import date

import ydb

from db.utils import execute_modifying_query


class AccountRepository:
    CLOSE_ACCOUNTS_QUERY = """
        declare $agreement_id as Text;
        declare $status as Text;
        declare $closing_date as Date;
            
        update account set
            status = $status,
            closing_date = $closing_date
        where agreement_id = $agreement_id;
    """
    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def insert_accounts(self, kwargs):
        column_types = ydb.BulkUpsertColumns() \
            .add_column("number", ydb.PrimitiveType.Utf8) \
            .add_column("agreement_id", ydb.PrimitiveType.Utf8) \
            .add_column("status", ydb.PrimitiveType.Utf8) \
            .add_column("opening_date", ydb.PrimitiveType.Date)

        self.__ydb_driver.table_client.bulk_upsert(
            os.getenv("YDB_DATABASE") + '/account',
            kwargs,
            column_types)

    def close_accounts(self, agreement_id):
        execute_modifying_query(
            pool=self.__ydb_pool,
            query=self.CLOSE_ACCOUNTS_QUERY,
            kwargs={
                "$agreement_id": agreement_id,
                "$status": "CLOSED",
                "$closing_date": date.today(),
            })
