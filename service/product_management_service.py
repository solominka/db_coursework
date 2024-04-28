import json
from datetime import date

import ydb

from db.repository.account_repository import AccountRepository
from db.repository.agreement_repository import AgreementRepository
from db.repository.client_repository import ClientRepository
from db.repository.request_repository import RequestRepository
from exceptions import ClientNotFoundException, AgreementNotFoundException
from service.id_generator import IdGenerator


class ProductManagementService:
    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

        self.__clientRepo = ClientRepository(ydb_driver)
        self.__agreementRepo = AgreementRepository(ydb_driver)
        self.__accountRepo = AccountRepository(ydb_driver)
        self.__request_repo = RequestRepository(ydb_driver)
        self.__id_generator = IdGenerator(ydb_driver)

    def create_product(self, request_body):
        agreement_id = self.__id_generator.generate_agreement_id(product=request_body['product'])
        buid = request_body['buid']
        client = self.__clientRepo.find_by_buid(buid=buid)
        if len(client) == 0:
            raise ClientNotFoundException("client not found by buid {}".format(buid))

        idempotency_token = request_body['idempotency_token']
        body_json = json.dumps(request_body)
        request = self.__request_repo.save_or_get(
            idempotency_token=idempotency_token,
            body=body_json,
            request_type="CREATE_PRODUCT",
        )
        if request is not None:
            return request['created_entity_id']

        account_numbers = self.__id_generator.generate_account_numbers(
            product=request_body['product'],
            auth_level=client[0]['auth_level'],
        )
        self.__agreementRepo.insert_agreement(
            id=agreement_id,
            buid=request_body['buid'],
            product=request_body['product'],
            status='OPEN',
            auth_level=client[0]['auth_level'],
        )
        self.__accountRepo.insert_accounts(
            kwargs=[{
                'buid': buid,
                'number': num,
                'agreement_id': agreement_id,
                'status': 'OPEN',
                'opening_date': date.today(),
                'auth_level': client[0]['auth_level'],
            } for num in account_numbers],
        )

        self.__request_repo.save_created_entity_id(
            idempotency_token=idempotency_token,
            created_entity_id=agreement_id,
        )
        return agreement_id

    def close_product(self, request_body):
        agreement_id = request_body['agreement_id']
        agreement = self.__agreementRepo.find_by_id(id=agreement_id)
        if len(agreement) == 0:
            raise AgreementNotFoundException("agreement not found by id {}".format(agreement_id))
        if agreement[0]['status'] == 'CLOSED':
            return

        idempotency_token = request_body['idempotency_token']
        body_json = json.dumps(request_body)
        request = self.__request_repo.save_or_get(
            idempotency_token=idempotency_token,
            body=body_json,
            request_type="CLOSE_PRODUCT",
        )
        if request is not None:
            return

        self.__agreementRepo.close_agreement(id=agreement_id)
        self.__accountRepo.close_accounts(agreement_id=agreement_id)

    def upgrade_products(self, buid, old_auth_level, new_auth_level):
        self.__agreementRepo.upgrade_current_account(
            buid=buid,
            auth_level=new_auth_level,
        )
        self.__accountRepo.batch_close_accounts(
            buid=buid,
            old_auth_level=old_auth_level,
        )
        open_agreements = self.__agreementRepo.find_by_buid(buid=buid)
        for ag in open_agreements:
            new_account_numbers = self.__id_generator.generate_account_numbers(
                product=ag['product'],
                auth_level=new_auth_level,
            )
            self.__accountRepo.insert_accounts(
                kwargs=[{
                    'buid': buid,
                    'number': num,
                    'agreement_id': ag['id'],
                    'status': 'OPEN',
                    'opening_date': date.today(),
                    'auth_level': new_auth_level,
                } for num in new_account_numbers],
            )

