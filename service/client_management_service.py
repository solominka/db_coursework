import json

from db.client_repository import ClientRepository
from db.request_repository import RequestRepository
from exceptions import IdempotencyViolationException


class ClientManagementService:
    def __init__(self, ydb_driver):
        self.__clientRepo = ClientRepository(ydb_driver)
        self.__request_repo = RequestRepository(ydb_driver)

    def register_user(self, request_body):
        idempotency_token = request_body['idempotency_token']
        body_json = json.dumps(request_body)
        existing_request = self.__request_repo.find_by_idempotency_token(idempotency_token)
        if len(existing_request) != 0:
            if existing_request[0]['body'] == body_json:
                return
            else:
                raise IdempotencyViolationException(message="idempotency violation on create client")
        else:
            self.__request_repo.save(idempotency_token=idempotency_token, body=body_json, request_type="CREATE_USER")

        self.__clientRepo.create_client(
            name=request_body['name'],
            surname=request_body['surname'],
            patronymic=request_body['patronymic'],
            phone=request_body['phone'],
            auth_level=request_body['auth_level'],
        )
