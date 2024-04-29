import json
from datetime import date

import ydb

from db.utils import execute_modifying_query, execute_reading_query


class CashbackRulesRepository:
    SAVE_CASHBACK_RULE_QUERY = """
        declare $id as Text;
        declare $buid as Text;
        declare $mcc_mapping as Text;
        declare $active_from as Date;
        declare $active_to as Date;
        
        insert into cashback_rules (id, buid, mcc_mapping, active_from, active_to) values
            ($id, $buid, cast($mcc_mapping as Json), $active_from, $active_to); 
    """

    GET_ACTIVE_RULES_FOR_BUID_QUERY = """
        declare $buid as Text;
        declare $at as Date;
        select * from cashback_rules 
        where 
            buid = $buid
            and active_from <= $at
            and active_to >= $at;
    """

    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def save_cashback_rule(self, rule):
        execute_modifying_query(
            pool=self.__ydb_pool,
            query=self.SAVE_CASHBACK_RULE_QUERY,
            kwargs={
                "$id": rule['id'],
                "$buid": rule['buid'],
                "$mcc_mapping": json.dumps({r['mcc']: r['rate'] for r in rule['mcc_mapping']}),
                "$active_from": date.fromisoformat(rule['active_from']),
                "$active_to": date.fromisoformat(rule['active_to']),
            })

    def get_active_rules_for_buid(self, buid, at):
        return execute_reading_query(
            pool=self.__ydb_pool,
            query=self.GET_ACTIVE_RULES_FOR_BUID_QUERY,
            kwargs={
                "$buid": buid,
                "$at": at,
            }
        )
