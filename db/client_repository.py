import os
from uuid import uuid4

import ydb


class ClientRepository:
    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def create_client(self, name, surname, patronymic, phone, auth_level):
        column_types = ydb.BulkUpsertColumns() \
            .add_column("buid", ydb.PrimitiveType.Utf8) \
            .add_column("version", ydb.PrimitiveType.Int64) \
            .add_column("name", ydb.PrimitiveType.Utf8) \
            .add_column("surname", ydb.PrimitiveType.Utf8) \
            .add_column("patronymic", ydb.PrimitiveType.Utf8) \
            .add_column("phone", ydb.PrimitiveType.Utf8) \
            .add_column("auth_level", ydb.PrimitiveType.Utf8)

        kwargs = [{
            "buid": str(uuid4()),
            "version": 0,
            "name": name,
            "surname": surname,
            "patronymic": patronymic,
            "phone": phone,
            "auth_level": auth_level,
        }]

        self.__ydb_driver.table_client.bulk_upsert(
            os.getenv("YDB_DATABASE") + '/client',
            kwargs,
            column_types)
