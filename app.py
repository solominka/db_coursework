import logging
from flask import Flask, jsonify, request
from flasgger import APISpec, Swagger, fields, Schema
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.flask import FlaskPlugin

from db.pool import get_ydb_driver
from exceptions import IdempotencyViolationException, ClientNotFoundException, AgreementNotFoundException
from service.client_management_service import ClientManagementService
from service.product_management_service import ProductManagementService

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


class CreateEntityResponseSchema(Schema):
    success = fields.Bool(required=True)
    error = fields.Str(required=False)
    id = fields.Str(required=False)


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
                $ref: '#/definitions/CreateEntityResponse'
    """

    try:
        buid = clientManagementService.register_user(request_body=request.get_json())
        resp = {'success': True, 'id': buid}
    except IdempotencyViolationException:
        resp = {'success': False, 'error': 'IDEMPOTENCY_VIOLATION'}

    return jsonify(CreateEntityResponseSchema().dump(resp))


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
          title: ProductData
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
                $ref: '#/definitions/CreateEntityResponse'
    """
    try:
        agreement_id = productManagementService.create_product(request_body=request.get_json())
        resp = {'success': True, 'id': agreement_id}
    except ClientNotFoundException:
        resp = {'success': False, 'error': 'CLIENT_NOT_FOUND'}

    return jsonify(CreateEntityResponseSchema().dump(resp))


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
                $ref: '#/definitions/CreateEntityResponse'
    """
    try:
        productManagementService.close_product(request_body=request.get_json())
        resp = {'success': True}
    except AgreementNotFoundException:
        resp = {'success': False, 'error': 'AGREEMENT_NOT_FOUND'}
    except IdempotencyViolationException:
        resp = {'success': False, 'error': 'IDEMPOTENCY_VIOLATION'}

    return jsonify(CreateEntityResponseSchema().dump(resp))


template = spec.to_flasgger(
    app,
    definitions=[CreateEntityResponseSchema],
    paths=[register_client]
)

swag = Swagger(app, template=template)

ydb_driver = get_ydb_driver()
ydb_driver.wait(fail_fast=True, timeout=30)
clientManagementService = ClientManagementService(ydb_driver)
productManagementService = ProductManagementService(ydb_driver)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True)
