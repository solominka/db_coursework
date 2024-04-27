import json
from datetime import date

from db.repository.account_repository import AccountRepository
from db.repository.agreement_repository import AgreementRepository
from db.repository.client_repository import ClientRepository
from db.repository.request_repository import RequestRepository
from exceptions import ClientNotFoundException
from service.id_generator import IdGenerator


class ProductManagementService:
    def __init__(self, ydb_driver):
        self.__clientRepo = ClientRepository(ydb_driver)
        self.__agreementRepo = AgreementRepository(ydb_driver)
        self.__accountRepo = AccountRepository(ydb_driver)
        self.__request_repo = RequestRepository(ydb_driver)
        self.__id_generator = IdGenerator(ydb_driver)

    def create_product(self, request_body):
        buid = request_body['buid']
        client = self.__clientRepo.find_by_buid(buid)
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

        agreement_id = self.__id_generator.generate_agreement_id(product=request_body['product'])
        self.__agreementRepo.insert_agreement(
            id=agreement_id,
            buid=request_body['buid'],
            status='OPEN',
            auth_level=client[0]['auth_level'],
        )
        self.__request_repo.save_created_entity_id(
            idempotency_token=idempotency_token,
            created_entity_id=agreement_id,
        )

        account_numbers = self.__id_generator.generate_account_numbers(product=request_body['product'])
        self.__accountRepo.insert_accounts(
            kwargs=[{
                'number': num,
                'agreement_id': agreement_id,
                'status': 'OPEN',
                'opening_date': date.today(),
            } for num in account_numbers]
        )
        return agreement_id
