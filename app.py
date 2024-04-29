import logging
from flask import Flask, jsonify, request
from flasgger import APISpec, Swagger, fields, Schema
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.flask import FlaskPlugin

from db.pool import get_ydb_driver
from db.repository.balance_repository import BalanceRepository
from db.repository.transaction_stmt_repository import TransactionStmtRepository
from exceptions import IdempotencyViolationException, ClientNotFoundException, AgreementNotFoundException, \
    InvalidInputException
from service.CashbackService import CashbackService
from service.client_management_service import ClientManagementService
from service.product_management_service import ProductManagementService
from service.transaction_service import TransactionService

spec = APISpec(
    title='Online bank system',
    version='1.0.0',
    openapi_version='2.0',
    plugins=[
        FlaskPlugin(),
        MarshmallowPlugin(),
    ],
)

app = Flask(__name__)


class ResponseSchema(Schema):
    success = fields.Bool(required=True)
    error = fields.Str(required=False)
    id = fields.Str(required=False)


class ProductDataSchema(Schema):
    id = fields.Str(required=True)
    opening_date = fields.Str(required=True)
    product = fields.Str(required=True)
    auth_level = fields.Str(required=True)
    balance_account_number = fields.Str(required=True)


class ClientAgreementsResponseSchema(Schema):
    success = fields.Bool(required=True)
    error = fields.Str(required=False)
    buid = fields.Str(required=False)
    products = fields.List(fields.Nested(ProductDataSchema), required=False)


class BalanceResponseSchema(Schema):
    success = fields.Bool(required=True)
    error = fields.Str(required=False)
    balance = fields.Str(required=False)


class CashbackResponseSchema(Schema):
    success = fields.Bool(required=True)
    error = fields.Str(required=False)
    cashback = fields.Str(required=False)


class CashbackRuleSchema(Schema):
    mcc = fields.Str()
    rate = fields.Float()


@app.route('/client/register', methods=['POST'])
def register_client():
    """
    Register a new user
    ---
    description: Register a new user
    parameters:
      - name: body
        in: body
        required: true
        schema:
          title: ClientData
          required:
            - name
            - surname
            - phone
            - auth_level
            - idempotency_token
          properties:
            name:
              type: string
            surname:
              type: string
            patronymic:
              type: string
            phone:
              type: string
            auth_level:
              type: string
            idempotency_token:
              type: string
    responses:
        200:
            description: Registration result
            schema:
                $ref: '#/definitions/Response'
    """

    try:
        buid = clientManagementService.register_user(request_body=request.get_json())
        resp = {'success': True, 'id': buid}
    except IdempotencyViolationException:
        resp = {'success': False, 'error': 'IDEMPOTENCY_VIOLATION'}

    return jsonify(ResponseSchema().dump(resp))


@app.route('/product/open', methods=['POST'])
def open_product():
    """
    Open a new product (current account / savings account / credit)
    ---
    description: Open a new product (current account / savings account / credit)
    parameters:
      - name: body
        in: body
        required: true
        schema:
          title: ProductDataOnOpen
          required:
            - buid
            - product
            - idempotency_token
          properties:
            buid:
              type: string
            product:
              type: string
            idempotency_token:
              type: string
    responses:
        200:
            description: Opening result
            schema:
                $ref: '#/definitions/Response'
    """
    try:
        agreement_id = productManagementService.create_product(request_body=request.get_json())
        resp = {'success': True, 'id': agreement_id}
    except ClientNotFoundException:
        resp = {'success': False, 'error': 'CLIENT_NOT_FOUND'}
    except IdempotencyViolationException:
        resp = {'success': False, 'error': 'IDEMPOTENCY_VIOLATION'}

    return jsonify(ResponseSchema().dump(resp))


@app.route('/product/close', methods=['POST'])
def close_product():
    """
    Close product (current account / savings account / credit)
    ---
    description: Close product (current account / savings account / credit)
    parameters:
      - name: body
        in: body
        required: true
        schema:
          title: CloseData
          required:
            - agreement_id
            - idempotency_token
          properties:
            agreement_id:
              type: string
            idempotency_token:
              type: string
    responses:
        200:
            description: Closing result
            schema:
                $ref: '#/definitions/Response'
    """
    try:
        productManagementService.close_product(request_body=request.get_json())
        resp = {'success': True}
    except AgreementNotFoundException:
        resp = {'success': False, 'error': 'AGREEMENT_NOT_FOUND'}
    except IdempotencyViolationException:
        resp = {'success': False, 'error': 'IDEMPOTENCY_VIOLATION'}

    return jsonify(ResponseSchema().dump(resp))


@app.route('/client/upgrade', methods=['POST'])
def upgrade_client():
    """
    Upgrade client and all their products
    ---
    description: Upgrade client and all their products
    parameters:
      - name: body
        in: body
        required: true
        schema:
          title: UpgradeData
          required:
            - buid
            - idempotency_token
            - new_auth_level
          properties:
            buid:
              type: string
            new_auth_level:
              type: string
            idempotency_token:
              type: string
    responses:
        200:
            description: Closing result
            schema:
                $ref: '#/definitions/Response'
    """
    try:
        clientManagementService.upgrade_user(request_body=request.get_json())
        resp = {'success': True}
    except ClientNotFoundException:
        resp = {'success': False, 'error': 'CLIENT_NOT_FOUND'}
    except IdempotencyViolationException:
        resp = {'success': False, 'error': 'IDEMPOTENCY_VIOLATION'}

    return jsonify(ResponseSchema().dump(resp))


@app.route('/txn/import', methods=['POST'])
def import_txn():
    """
    Import transaction from originating system
    ---
    description: Import transaction from originating system
    parameters:
      - name: body
        in: body
        required: true
        schema:
          title: Transaction
          required:
            - id
            - mcc
            - status
            - iso_direction
            - iso_class
            - iso_category
            - transaction_date
            - rrn
            - orn
            - transaction_amount
          properties:
            id:
              type: string
            ref_id:
              type: string
            mcc:
              type: string
            authorization_id:
              type: string
            status:
              type: string
            iso_direction:
              type: string
            iso_class:
              type: string
            iso_category:
              type: string
            transaction_date:
              type: string
            rrn:
              type: string
            orn:
              type: string
            transaction_amount:
              type: string
            receiver_agreement_id:
              type: string
            originator_agreement_id:
              type: string
    responses:
        200:
            description: Result
            schema:
                $ref: '#/definitions/Response'
    """
    try:
        transactionService.import_txn(txn=request.get_json())
        resp = {'success': True}
    except AgreementNotFoundException:
        resp = {'success': False, 'error': 'AGREEMENT_NOT_FOUND'}

    return jsonify(ResponseSchema().dump(resp))


@app.route('/cashback/rules/set', methods=['POST'])
def set_cashback_rules():
    """
        Set cashback rules for client
        ---
        description: Set cashback rules for client
        parameters:
          - name: body
            in: body
            required: true
            schema:
              title: CashbackRuleData
              required:
                - buid
                - mcc_mapping
                - active_from
                - active_to
                - idempotency_token
              properties:
                buid:
                  type: string
                active_from:
                  type: string
                active_to:
                  type: string
                mcc_mapping:
                  type: array
                  items:
                    $ref: '#/definitions/CashbackRule'
                idempotency_token:
                  type: string
        responses:
            200:
                description: Result
                schema:
                    $ref: '#/definitions/Response'
        """
    try:
        rule_id = cashbackService.save_rule(rule=request.get_json())
        resp = {'success': True, 'id': rule_id}
    except ClientNotFoundException:
        resp = {'success': False, 'error': 'CLIENT_NOT_FOUND'}
    except InvalidInputException as e:
        resp = {'success': False, 'error': str(e)}
    except IdempotencyViolationException:
        resp = {'success': False, 'error': 'IDEMPOTENCY_VIOLATION'}

    return jsonify(ResponseSchema().dump(resp))


@app.route('/agreement/balance/<agreement_id>', methods=['GET'])
def get_agreement_balance(agreement_id):
    """
    Get agreement balance
    ---
    description: Get agreement balance
    parameters:
      - name: agreement_id
        in: path
        required: true
        type: string
    responses:
        200:
            description: Result
            schema:
                $ref: '#/definitions/BalanceResponse'
    """
    try:
        balance = balanceRepository.get_balance(agreement_id=agreement_id)
        resp = {'success': True, 'balance': balance}
    except AgreementNotFoundException:
        resp = {'success': False, 'error': 'AGREEMENT_NOT_FOUND'}

    return jsonify(BalanceResponseSchema().dump(resp))


@app.route('/agreement/cashback/<agreement_id>', methods=['GET'])
def get_agreement_cashback(agreement_id):
    """
    Get agreement cashback
    ---
    description: Get agreement cashback
    parameters:
      - name: agreement_id
        in: path
        required: true
        type: string
    responses:
        200:
            description: Result
            schema:
                $ref: '#/definitions/CashbackResponse'
    """
    try:
        cashback = balanceRepository.get_cashback(agreement_id=agreement_id)
        resp = {'success': True, 'cashback': cashback}
    except AgreementNotFoundException:
        resp = {'success': False, 'error': 'AGREEMENT_NOT_FOUND'}

    return jsonify(CashbackResponseSchema().dump(resp))


@app.route('/client/<buid>/products', methods=['GET'])
def get_client_products(buid):
    """
    Get client agreements
    ---
    description: Get agreement balance
    parameters:
      - name: buid
        in: path
        required: true
        type: string
    responses:
        200:
            description: Result
            schema:
                $ref: '#/definitions/ClientAgreementsResponse'
    """
    try:
        products = productManagementService.get_client_products(buid)
        resp = {'success': True, 'products': products, 'buid': buid}
    except ClientNotFoundException:
        resp = {'success': False, 'error': 'CLIENT_NOT_FOUND'}

    return jsonify(ClientAgreementsResponseSchema().dump(resp))


@app.route('/transaction/<txn_id>/stmt', methods=['GET'])
def get_transaction_stmt(txn_id):
    """
    Get transaction statement
    ---
    description: Get transaction statement
    parameters:
      - name: txn_id
        in: path
        required: true
        type: string
    responses:
        200:
            description: Result
            schema:
                $ref: '#/definitions/Response'
    """
    try:
        transactionStmtRepository.get_file(key=txn_id)
        resp = {'success': True}
    except AgreementNotFoundException:
        resp = {'success': False, 'error': 'TRANSACTION_NOT_FOUND'}

    return jsonify(CashbackResponseSchema().dump(resp))


template = spec.to_flasgger(
    app,
    definitions=[ResponseSchema, BalanceResponseSchema, CashbackRuleSchema, CashbackResponseSchema,
                 ClientAgreementsResponseSchema],
    paths=[register_client, open_product, close_product, upgrade_client, import_txn, set_cashback_rules,
           get_agreement_balance, get_client_products, get_transaction_stmt]
)

swag = Swagger(app, template=template)

ydb_driver = get_ydb_driver()
ydb_driver.wait(fail_fast=True, timeout=30)
clientManagementService = ClientManagementService(ydb_driver)
productManagementService = ProductManagementService(ydb_driver)
transactionService = TransactionService(ydb_driver)
balanceRepository = BalanceRepository()
cashbackService = CashbackService(ydb_driver)
transactionStmtRepository = TransactionStmtRepository()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True)
