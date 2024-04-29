import json
from datetime import date
from uuid import uuid4

import ydb

from db.repository.balance_repository import BalanceRepository
from db.repository.cashback_rules_repository import CashbackRulesRepository
from db.repository.client_repository import ClientRepository
from db.repository.request_repository import RequestRepository
from exceptions import ClientNotFoundException, InvalidInputException


class CashbackService:
    def __init__(self, ydb_driver):
        self.__ydb_pool = ydb.SessionPool(ydb_driver)
        self.__clientRepo = ClientRepository(ydb_driver)
        self.__cashbackRulesRepo = CashbackRulesRepository(ydb_driver)
        self.__request_repo = RequestRepository(ydb_driver)
        self.__balanceRepository = BalanceRepository()

    def save_rule(self, rule):
        if date.fromisoformat(rule['active_from']) < date.today():
            raise InvalidInputException("active_from shouldn't be in the past")

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

    def credit_cashback(self, buid, agreement_id, mcc, txn_amount, txn_date):
        rules = self.__cashbackRulesRepo.get_active_rules_for_buid(buid=buid, at=date.fromisoformat(txn_date))
        rate = 0.0
        for rule in rules:
            mcc_mapping = json.loads(rule['mcc_mapping'])
            rate += mcc_mapping.get(mcc, 0)

        self.__balanceRepository.credit_cashback(agreement_id=agreement_id, cashback=float(txn_amount) * rate / 100)

    def debit_cashback(self, buid, agreement_id, mcc, txn_amount, txn_date):
        rules = self.__cashbackRulesRepo.get_active_rules_for_buid(buid=buid, at=date.fromisoformat(txn_date))
        rate = 0.0
        for rule in rules:
            mcc_mapping = json.loads(rule['mcc_mapping'])
            rate += mcc_mapping.get(mcc, 0)

        self.__balanceRepository.credit_cashback(agreement_id=agreement_id, cashback=-txn_amount * rate / 100)
