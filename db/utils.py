import ydb


# using prepared statements
# https://ydb.tech/en/docs/reference/ydb-sdk/example/python/#param-prepared-queries
def execute_select_query(pool, query, kwargs):
    def callee(session):
        prepared_query = session.prepare(query)
        result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
            prepared_query, kwargs, commit_tx=True
        )
        return result_sets[0].rows

    return pool.retry_operation_sync(callee)


def execute_update_query(pool, query, kwargs):
    def callee(session):
        prepared_query = session.prepare(query)
        session.transaction().execute(
            prepared_query, kwargs, commit_tx=True
        )

    pool.retry_operation_sync(callee)
