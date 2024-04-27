class IdempotencyViolationException(Exception):
    def __init__(self, message):
        self.message = message

    def str(self):
        return self.message
