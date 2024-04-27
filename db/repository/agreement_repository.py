import os
from datetime import date

import ydb


class AgreementRepository:
    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def insert_agreement(self, id, buid, status, auth_level):
        column_types = ydb.BulkUpsertColumns() \
            .add_column("id", ydb.PrimitiveType.Utf8) \
            .add_column("buid", ydb.PrimitiveType.Utf8) \
            .add_column("status", ydb.PrimitiveType.Utf8) \
            .add_column("opening_date", ydb.PrimitiveType.Date) \
            .add_column("auth_level", ydb.PrimitiveType.Utf8)

        kwargs = [{
            "id": id,
            "buid": buid,
            "status": status,
            "opening_date": date.today(),
            "auth_level": auth_level,
        }]

        self.__ydb_driver.table_client.bulk_upsert(
            os.getenv("YDB_DATABASE") + '/agreement',
            kwargs,
            column_types)
