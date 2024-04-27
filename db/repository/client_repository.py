import os
import ydb

from db.utils import execute_select_query


class ClientRepository:
    SELECT_BY_BUID_QUERY = """
            declare $buid as Text;
            select * from client where buid = $buid; 
        """

    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def find_by_buid(self, buid):
        return execute_select_query(
            pool=self.__ydb_pool,
            query=self.SELECT_BY_BUID_QUERY,
            kwargs={
                "$buid": buid
            })

    def insert_client(self, buid, name, surname, patronymic, phone, auth_level):
        column_types = ydb.BulkUpsertColumns() \
            .add_column("buid", ydb.PrimitiveType.Utf8) \
            .add_column("version", ydb.PrimitiveType.Int64) \
            .add_column("name", ydb.PrimitiveType.Utf8) \
            .add_column("surname", ydb.PrimitiveType.Utf8) \
            .add_column("patronymic", ydb.PrimitiveType.Utf8) \
            .add_column("phone", ydb.PrimitiveType.Utf8) \
            .add_column("auth_level", ydb.PrimitiveType.Utf8)

        kwargs = [{
            "buid": buid,
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
