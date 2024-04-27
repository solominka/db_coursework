import logging
from flask import Flask, jsonify, request
from flasgger import APISpec, Swagger, fields, Schema
from apispec.ext.marshmallow import MarshmallowPlugin
from apispec_webframeworks.flask import FlaskPlugin

from db.pool import get_ydb_driver
from exceptions import IdempotencyViolationException
from service.client_management_service import ClientManagementService

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


class RegisterResponseSchema(Schema):
    success = fields.Bool(required=True)
    error = fields.Str(required=False)


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
                $ref: '#/definitions/RegisterResponse'
    """

    try:
        clientManagementService.register_user(request_body=request.get_json())
        resp = {'success': True}
    except IdempotencyViolationException:
        resp = {'success': False, 'error': 'IDEMPOTENCY_VIOLATION'}

    return jsonify(RegisterResponseSchema().dump(resp))


template = spec.to_flasgger(
    app,
    definitions=[RegisterResponseSchema],
    paths=[register_client]
)

swag = Swagger(app, template=template)

ydb_driver = get_ydb_driver()
ydb_driver.wait(fail_fast=True, timeout=30)
clientManagementService = ClientManagementService(ydb_driver)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True)
