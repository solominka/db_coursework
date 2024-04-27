import ydb

from db.utils import execute_select_query


class AccountNumberSequenceRepository:
    GET_NEXT_NUMBERS_QUERY = """
            declare $balance_positions as List<Utf8>;
            update account_number_sequence set current_value = current_value + 1 where balance_position in $balance_positions;
            select * from account_number_sequence where balance_position in $balance_positions; 
        """

    def __init__(self, ydb_driver):
        self.__ydb_driver = ydb_driver
        self.__ydb_pool = ydb.SessionPool(self.__ydb_driver)

    def get_next_numbers(self, balance_positions):
        return execute_select_query(
            pool=self.__ydb_pool,
            query=self.GET_NEXT_NUMBERS_QUERY,
            kwargs={
                "$balance_positions": balance_positions,
            }
        )
