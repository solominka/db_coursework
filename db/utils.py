import ydb


# using prepared statements
# https://ydb.tech/en/docs/reference/ydb-sdk/example/python/#param-prepared-queries
def execute_reading_query(pool, query, kwargs, current_transaction=None, commit_tx=True):
    def callee(session):
        s = session
        if current_transaction is not None:
            s = current_transaction.session

        tx = current_transaction
        if tx is None:
            tx = s.transaction(ydb.SerializableReadWrite())

        prepared_query = s.prepare(query)
        result_sets = tx.execute(
            prepared_query, kwargs, commit_tx=commit_tx
        )
        return result_sets[0].rows

    return pool.retry_operation_sync(callee)


def execute_modifying_query(pool, query, kwargs, current_transaction=None, commit_tx=True):
    def callee(session):
        s = session
        if current_transaction is not None:
            s = current_transaction.session

        tx = current_transaction
        if tx is None:
            tx = s.transaction()

        prepared_query = s.prepare(query)
        tx.execute(
            prepared_query, kwargs, commit_tx=commit_tx
        )

    pool.retry_operation_sync(callee)
