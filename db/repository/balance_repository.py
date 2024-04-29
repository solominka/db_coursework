import os
from rediscluster import RedisCluster

from exceptions import AgreementNotFoundException


class BalanceRepository:
    def __init__(self):
        hosts = [
            'rc1a-dc1f9jr7h014molt.mdb.yandexcloud.net',
            'rc1a-s76vld31vtcqa8kc.mdb.yandexcloud.net',
            'rc1b-nfjn3pt8j462bo2v.mdb.yandexcloud.net'
        ]
        startup_nodes = [{"host": h, "port": 6380} for h in hosts]
        self.__rc = RedisCluster(
            startup_nodes=startup_nodes,
            decode_responses=True,
            skip_full_coverage_check=True,
            password=os.getenv("REDIS_PASSWORD"),
            ssl=True,
            ssl_ca_certs=os.getenv("SSL_SERT_FILEPATH"),
        )

    def update_balances(self, agreement_id, balance_change):
        current_balance = self.__rc.get(agreement_id)
        if current_balance is None:
            raise AgreementNotFoundException("agreement not found by id {}".format(agreement_id))
        else:
            current_balance = int(current_balance)

        self.__rc.set(agreement_id, current_balance + balance_change)

    def init_agreement(self, agreement_id):
        self.__rc.set(agreement_id, 0)

    def get_balance(self, agreement_id):
        return self.__rc.get(agreement_id)
