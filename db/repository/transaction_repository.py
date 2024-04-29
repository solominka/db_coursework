from datetime import datetime, date

import ydb

from db.utils import execute_modifying_query


class TransactionRepository:
    SAVE_TRANSACTION_QUERY = """
        declare $id as Text;
        declare $ref_id as Text;
        declare $authorization_id as Text;
        declare $status as Text;
        declare $isoDirection as Text;
        declare $isoClass as Text;
        declare $isoCategory as Text;
        declare $transactionDate as Date;
        declare $rrn as Text;
        declare $orn as Text;
        declare $transaction_amount as Text;
        declare $receiver_agreement_id as Text;
        declare $originator_agreement_id as Text;
        declare $created_at as Timestamp;
        
        insert into transaction_event(id, ref_id, mcc, authorization_id, status, isoDirection, isoClass, isoCategory, 
            transactionDate, rrn, orn, transaction_amount, receiver_agreement_id, originator_agreement_id, created_at) 
        values
            ($id, $ref_id, $mcc, $authorization_id, $status, $isoDirection, $isoClass, $isoCategory, $transactionDate,
                $rrn, $orn, $transaction_amount, $receiver_agreement_id, $originator_agreement_id, $created_at);
    """

    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def save_transaction(self, txn):
        execute_modifying_query(
            pool=self.__ydb_pool,
            query=self.SAVE_TRANSACTION_QUERY,
            kwargs={
                "$id": txn['id'],
                "$ref_id": txn.get('ref_id', ''),
                '$mcc': txn['mcc'],
                "$authorization_id": txn.get('authorization_id', ''),
                "$status": txn['status'],
                "$isoDirection": txn['iso_direction'],
                "$isoClass": txn['iso_class'],
                "$isoCategory": txn['iso_category'],
                "$transactionDate": date.fromisoformat(txn['transaction_date']),
                "$rrn": txn['rrn'],
                "$orn": txn['orn'],
                "$transaction_amount": txn['transaction_amount'],
                "$receiver_agreement_id": txn.get('receiver_agreement_id', ''),
                "$originator_agreement_id": txn.get('originator_agreement_id', ''),
                "$created_at": datetime.now(),
            })
