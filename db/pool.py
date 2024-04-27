import os
import ydb


def get_ydb_pool(timeout=30):
    ydb_driver = get_ydb_driver()
    ydb_driver.wait(fail_fast=True, timeout=timeout)
    return ydb.SessionPool(ydb_driver)


def get_ydb_driver():
    return ydb.Driver(
        endpoint=os.getenv("YDB_ENDPOINT"),
        database=os.getenv("YDB_DATABASE"),
        credentials=ydb.iam.ServiceAccountCredentials.from_file(
            os.getenv("SA_KEY_FILE"),
        ),
    )
