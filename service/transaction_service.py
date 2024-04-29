import ydb

from db.repository.agreement_repository import AgreementRepository
from db.repository.balance_repository import BalanceRepository
from db.repository.client_repository import ClientRepository
from db.repository.request_repository import RequestRepository
from db.repository.transaction_repository import TransactionRepository
from exceptions import InvalidTransactionException
from service.CashbackService import CashbackService
from service.transaction_stmt_service import TransactionStmtService


class TransactionService:
    STATUSES = ['HOLD', 'CLEAR', 'CANCEL', 'FAIL']
    ISO_DIRECTIONS = ['CREDIT', 'DEBIT']
    ISO_CLASSES = ['AUTHORIZATION', 'FINANCIAL']
    ISO_CATEGORIES = ['REQUEST', 'ADVICE', 'CHECK', 'NOTIFICATION', 'ACKNOWLEDGEMENT', 'NEGATIVE_ACKNOWLEDGEMENT']

    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

        self.__clientRepo = ClientRepository(ydb_driver)
        self.__agreementRepo = AgreementRepository(ydb_driver)
        self.__request_repo = RequestRepository(ydb_driver)
        self.__transactionRepo = TransactionRepository(ydb_driver)
        self.__balanceRepository = BalanceRepository()
        self.__cashbackService = CashbackService(ydb_driver)
        self.__transactionStmtService = TransactionStmtService(ydb_driver)

    def import_txn(self, txn):
        error = self.validate_txn(txn=txn)
        if error is not None:
            raise InvalidTransactionException(message=error)

        try:
            self.__transactionRepo.save_transaction(txn=txn)
        except ydb.issues.PreconditionFailed:
            return
        self.update_balance(txn=txn)
        self.__transactionStmtService.save_stmt(txn=txn)

    def validate_txn(self, txn):
        if txn['status'] not in self.STATUSES:
            return "invalid status"
        if txn['iso_direction'] not in self.ISO_DIRECTIONS:
            return "invalid direction"
        if txn['iso_class'] not in self.ISO_CLASSES:
            return "invalid iso_class"
        if txn['iso_category'] not in self.ISO_CATEGORIES:
            return "invalid iso_category"

    def update_balance(self, txn):
        if txn['iso_class'] != 'FINANCIAL' or txn['status'] in ['CLEAR', 'FAIL'] \
                or txn['iso_category'] not in ['REQUEST', 'ADVICE']:
            return

        balance_change = float(txn['transaction_amount'])
        if txn['iso_direction'] == 'CREDIT':  # пополнение
            agreement_id = txn['receiver_agreement_id']
        else:  # списание
            agreement_id = txn['originator_agreement_id']
            balance_change *= -1

        if txn['status'] == 'CANCEL':
            balance_change *= -1

        agreement = self.__agreementRepo.find_by_id(agreement_id)
        if len(agreement) == 0:
            return

        self.__balanceRepository.update_balances(agreement_id=agreement_id, balance_change=balance_change)
        if balance_change < 0 and txn['status'] != 'CANCEL':
            self.__cashbackService.credit_cashback(
                agreement_id=agreement_id,
                buid=agreement[0]['buid'],
                mcc=txn['mcc'],
                txn_amount=txn['transaction_amount'],
                txn_date=txn['transaction_date'],
            )

        if balance_change > 0 and txn['status'] == 'CANCEL':
            original_transaction = self.__transactionRepo.find_by_id(id=txn['ref_id'])
            self.__cashbackService.debit_cashback(
                agreement_id=agreement_id,
                buid=agreement[0]['buid'],
                mcc=original_transaction['mcc'],
                txn_amount=balance_change,
                txn_date=original_transaction['transaction_date'],
            )
