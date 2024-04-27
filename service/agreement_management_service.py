import json

from db.repository.agreement_repository import AgreementRepository
from db.repository.client_repository import ClientRepository
from db.repository.request_repository import RequestRepository
from exceptions import ClientNotFoundException
from service.id_generator import IdGenerator


class AgreementManagementService:
    def __init__(self, ydb_driver):
        self.__clientRepo = ClientRepository(ydb_driver)
        self.__agreementRepo = AgreementRepository(ydb_driver)
        self.__request_repo = RequestRepository(ydb_driver)
        self.__id_generator = IdGenerator()

    def create_agreement(self, request_body):
        buid = request_body['buid']
        client = self.__clientRepo.find_by_buid(buid)
        if len(client) == 0:
            raise ClientNotFoundException("client not found by buid {}".format(buid))

        idempotency_token = request_body['idempotency_token']
        body_json = json.dumps(request_body)
        request = self.__request_repo.save_or_get(
            idempotency_token=idempotency_token,
            body=body_json,
            request_type="CREATE_AGREEMENT",
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
        return agreement_id
