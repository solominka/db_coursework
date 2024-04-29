import json
from uuid import uuid4

import ydb

from db.repository.account_repository import AccountRepository
from db.repository.agreement_repository import AgreementRepository
from db.repository.client_repository import ClientRepository
from db.repository.lock_repository import LockRepository
from db.repository.request_repository import RequestRepository
from service.product_management_service import ProductManagementService


class ClientManagementService:
    def __init__(self, ydb_driver):
        self.__ydb_pool = ydb.SessionPool(ydb_driver)
        self.__clientRepo = ClientRepository(ydb_driver)
        self.__agreementRepo = AgreementRepository(ydb_driver)
        self.__accountRepo = AccountRepository(ydb_driver)
        self.__request_repo = RequestRepository(ydb_driver)
        self.__productManagementService = ProductManagementService(ydb_driver)
        self.__lockRepo = LockRepository(ydb_driver)

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
        self.__request_repo.save_result(
            idempotency_token=idempotency_token,
            created_entity_id=buid,
        )
        return buid

    def upgrade_user(self, request_body):
        buid = request_body['buid']
        idempotency_token = request_body['idempotency_token']
        body_json = json.dumps(request_body)
        request = self.__request_repo.save_or_get(
            idempotency_token=idempotency_token,
            body=body_json,
            request_type="UPGRADE_USER",
        )
        if request is not None:
            return

        self.__lockRepo.acquire_lock(buid=buid)

        new_auth_level = request_body['new_auth_level']
        old_auth_level = self.__clientRepo.find_by_buid(buid=buid)[0]['auth_level']
        if int(old_auth_level) >= int(new_auth_level):
            self.__lockRepo.release_lock(buid=buid)
            return

        self.__clientRepo.update_auth_level_by_buid(
            buid=buid,
            new_auth_level=request_body['new_auth_level'],
        )
        self.__productManagementService.upgrade_products(
            buid=buid,
            old_auth_level=old_auth_level,
            new_auth_level=request_body['new_auth_level'],
        )
        self.__lockRepo.release_lock(buid=buid)
