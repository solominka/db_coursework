import shortuuid

from db.repository.account_number_sequence_repository import AccountNumberSequenceRepository


class IdGenerator:
    AGREEMENT_ID_PREFIXES = {
        'current_account': 'ca_',
        'savings_account': 'sa_'
    }

    ACCOUNT_BALANCE_POSITIONS = {
        'current_account': ['47423', '40903'],
        'savings_account': ['42301', '47411', '47422', '47423']
    }

    def __init__(self, ydb_driver):
        self.__accountNumberSequenceRepository = AccountNumberSequenceRepository(ydb_driver=ydb_driver)

    def generate_agreement_id(self, product):
        return self.AGREEMENT_ID_PREFIXES[product] + shortuuid.uuid()

    def generate_account_numbers(self, product, tx=None):
        balance_positions = self.ACCOUNT_BALANCE_POSITIONS[product]
        account_numbers = self.__accountNumberSequenceRepository.get_next_numbers(balance_positions=balance_positions,
                                                                                  tx=tx)
        return [it['balance_position'] + str(it['current_value']).zfill(15) for it in account_numbers]
