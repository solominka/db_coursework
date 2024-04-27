import shortuuid


class IdGenerator:
    PRODUCT_PREFIXES = {
        'current_account': 'ca_',
        'savings_account': 'sa_'
    }

    def generate_agreement_id(self, product):
        return self.PRODUCT_PREFIXES[product] + shortuuid.uuid()
