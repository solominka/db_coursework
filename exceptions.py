class IdempotencyViolationException(Exception):
    def __init__(self, message):
        self.message = message

    def str(self):
        return self.message


class ClientNotFoundException(Exception):
    def __init__(self, message):
        self.message = message

    def str(self):
        return self.message


class AgreementNotFoundException(Exception):
    def __init__(self, message):
        self.message = message

    def str(self):
        return self.message


class InvalidTransactionException(Exception):
    def __init__(self, message):
        self.message = message

    def str(self):
        return self.message


class InvalidInputException(Exception):
    def __init__(self, message):
        self.message = message

    def str(self):
        return self.message


class ConcurrentModificationException(Exception):
    def __init__(self, message):
        self.message = message

    def str(self):
        return self.message
