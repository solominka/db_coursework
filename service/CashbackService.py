import json
from uuid import uuid4

import ydb

from db.repository.cashback_rules_repository import CashbackRulesRepository
from db.repository.client_repository import ClientRepository
from db.repository.request_repository import RequestRepository
from exceptions import ClientNotFoundException


class CashbackService:
    def __init__(self, ydb_driver):
        self.__ydb_pool = ydb.SessionPool(ydb_driver)
        self.__clientRepo = ClientRepository(ydb_driver)
        self.__cashbackRulesRepo = CashbackRulesRepository(ydb_driver)
        self.__request_repo = RequestRepository(ydb_driver)

    def save_rule(self, rule):
        buid = rule['buid']
        client = self.__clientRepo.find_by_buid(buid=buid)
        if len(client) == 0:
            raise ClientNotFoundException("client not found by buid {}".format(buid))

        idempotency_token = rule['idempotency_token']
        body_json = json.dumps(rule)
        request = self.__request_repo.save_or_get(
            idempotency_token=idempotency_token,
            body=body_json,
            request_type="CREATE_CASHBACK_RULE",
        )
        if request is not None:
            return request['created_entity_id']

        rule_id = str(uuid4())
        rule['id'] = rule_id
        self.__cashbackRulesRepo.save_cashback_rule(rule)
        self.__request_repo.save_created_entity_id(
            idempotency_token=idempotency_token,
            created_entity_id=rule_id,
        )
        return rule_id
