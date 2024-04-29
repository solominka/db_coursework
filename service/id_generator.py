import shortuuid

from db.repository.account_number_sequence_repository import AccountNumberSequenceRepository


class IdGenerator:
    AGREEMENT_ID_PREFIXES = {
        'current_account': 'ca_',
        'savings_account': 'sa_'
    }

    BALANCE_POSITIONS_TO_TYPES = {
        '47423': 'BANK_FEES',
        '40903': 'BALANCE',
        '40914': 'BALANCE',
        '42301': 'BALANCE',
        '47411': 'INTEREST_CAPITALIZATION',
        '47422': 'BANK_LIABILITY',

    }

    ACCOUNT_BALANCE_POSITIONS = {
        'current_account':
            {
                "1": ['47423', '40903'],
                "2": ['47423', '40914'],
            },
        'savings_account': {
                "1": ['42301', '47411', '47422', '47423'],
                "2": ['42301', '47411', '47422', '47423'],
            }
    }

    def __init__(self, ydb_driver):
        self.__accountNumberSequenceRepository = AccountNumberSequenceRepository(ydb_driver=ydb_driver)

    def generate_agreement_id(self, product):
        return self.AGREEMENT_ID_PREFIXES[product] + shortuuid.uuid()

    def generate_account_numbers(self, product, auth_level):
        balance_positions = self.ACCOUNT_BALANCE_POSITIONS[product][auth_level]
        account_numbers = self.__accountNumberSequenceRepository.get_next_numbers(balance_positions=balance_positions)
        return [{
            'number': it['balance_position'] + str(it['current_value']).zfill(15),
            'type': self.BALANCE_POSITIONS_TO_TYPES[it['balance_position']],
        } for it in account_numbers]
