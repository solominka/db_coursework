import os
import ydb

from db.utils import execute_reading_query, execute_modifying_query


class ClientRepository:
    SELECT_BY_BUID_QUERY = """    
        declare $buid as Text;
        select * from client where buid = $buid; 
    """

    SAVE_CLIENT_QUERY = """
        declare $buid as Text;
        declare $version as Int64;
        declare $name as Text;
        declare $surname as Text;
        declare $patronymic as Text;
        declare $phone as Text;
        declare $auth_level as Text;
        insert into client(buid, version, name, surname, patronymic, phone, auth_level) values
            ($buid, $version, $name, $surname, $patronymic, $phone, $auth_level);
    """

    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def find_by_buid(self, buid, tx=None):
        return execute_reading_query(
            pool=self.__ydb_pool,
            current_transaction=tx,
            commit_tx=False,
            query=self.SELECT_BY_BUID_QUERY,
            kwargs={
                "$buid": buid
            })

    def insert_client(self, buid, name, surname, patronymic, phone, auth_level):
        execute_modifying_query(
            pool=self.__ydb_pool,
            query=self.SAVE_CLIENT_QUERY,
            kwargs={
                "$buid": buid,
                "$version": 0,
                "$name": name,
                "$surname": surname,
                "$patronymic": patronymic,
                "$phone": phone,
                "$auth_level": auth_level,
            })
