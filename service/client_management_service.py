import json
from uuid import uuid4

from db.repository.client_repository import ClientRepository
from db.repository.request_repository import RequestRepository


class ClientManagementService:
    def __init__(self, ydb_driver):
        self.__clientRepo = ClientRepository(ydb_driver)
        self.__request_repo = RequestRepository(ydb_driver)

    def register_user(self, request_body):
        idempotency_token = request_body['idempotency_token']
        body_json = json.dumps(request_body)
        request = self.__request_repo.save_or_get(
            idempotency_token=idempotency_token,
            body=body_json,
            request_type="CREATE_USER",
        )
        if request is not None:
            return request['created_entity_id']

        buid = str(uuid4())
        self.__clientRepo.insert_client(
            buid=buid,
            name=request_body['name'],
            surname=request_body['surname'],
            patronymic=request_body['patronymic'],
            phone=request_body['phone'],
            auth_level=request_body['auth_level'],
        )
        self.__request_repo.save_created_entity_id(
            idempotency_token=idempotency_token,
            created_entity_id=buid,
        )
        return buid
