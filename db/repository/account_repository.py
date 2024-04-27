import os

import ydb


class AccountRepository:
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
