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

    UPDATE_AUTH_LEVEL_QUERY = """
        declare $buid as Text;
        declare $new_auth_level as Text;
        
        update client set
            auth_level = $new_auth_level,
            version = version + 1
        where buid = $buid;
    """

    FIND_CLIENT_BY_AGREEMENT_ID_QUERY = """
        declare $agreement_id as Text;
        
        select * from client where buid in (
            select buid from agreement where id = $agreement_id
        );
    """

    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def find_by_buid(self, buid):
        return execute_reading_query(
            pool=self.__ydb_pool,
            query=self.SELECT_BY_BUID_QUERY,
            kwargs={
                "$buid": buid
            })

    def find_by_agreement_id(self, agreement_id):
        return execute_reading_query(
            pool=self.__ydb_pool,
            query=self.FIND_CLIENT_BY_AGREEMENT_ID_QUERY,
            kwargs={
                "$agreement_id": agreement_id
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

    def update_auth_level_by_buid(self, buid, new_auth_level):
        execute_modifying_query(
            pool=self.__ydb_pool,
            query=self.UPDATE_AUTH_LEVEL_QUERY,
            kwargs={
                "$buid": buid,
                "$new_auth_level": new_auth_level,
            })
