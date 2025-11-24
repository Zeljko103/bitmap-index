from typing import Optional, List, Tuple, Dict
from enum import Enum


class AggregateFunction(Enum):
    MIN = 'min'
    MAX = 'max'
    AVG = 'avg'
    SUM = 'sum'
    COUNT = 'count'


class Table:
    def __init__(self, name: str, columns: List[str], data: Optional[List[List[str]]] = None) -> None:
        self._name: str = name
        self._columns: List[str] = columns  # column names
        self._data: List[List[str]] = data or []  # list of rows

    @property
    def name(self) -> str:
        return self._name

    @property
    def columns(self) -> List[str]:
        return self._columns

    @property
    def data(self) -> List[List[str]]:
        return self._data

    @data.setter
    def data(self, value: Optional[List[List[str]]]) -> None:
        self._data = value or []

    def find_column_index(self, column_name: str) -> int:
        try:
            return self._columns.index(column_name)
        except ValueError:
            raise ValueError(f"Column '{column_name}' does not exist in the table '{self._name}'.")


class MainTable(Table):
    def __init__(self, name: str, columns: List[str], data: Optional[List[List[str]]] = None) -> None:
        super().__init__(name, columns, data)
        self._bitmap_indexes: List[Dict[str, List[int]]] = []

    @property
    def list_of_bitmap_indexes(self) -> List[Dict[str, List[int]]]:
        return self._bitmap_indexes

    @list_of_bitmap_indexes.setter
    def list_of_bitmap_indexes(self, value: List[Dict[str, List[int]]]) -> None:
        self._bitmap_indexes = value

    @staticmethod
    def __is_number(s: str) -> bool:
        try:
            float(s)
            return True
        except ValueError:
            return False

    @staticmethod
    def __apply_single_aggregate(column_data: List[float], func: AggregateFunction) -> float:
        if func == AggregateFunction.MIN:
            return min(column_data)
        elif func == AggregateFunction.MAX:
            return max(column_data)
        elif func == AggregateFunction.AVG:
            return sum(column_data) / len(column_data)
        elif func == AggregateFunction.SUM:
            return sum(column_data)
        elif func == AggregateFunction.COUNT:
            return len(column_data)
        else:
            raise ValueError(f"Unknown aggregate function {func}.")

    @staticmethod
    def format_conditions(multiple_lists_of_conditions: List[List[Tuple[str, str]]],
                          list_of_tables: List[Table]) -> List[Dict[str, Tuple[str, ...]]]:
        """
        Formatting the conditions to get a desired output
        :param multiple_lists_of_conditions:
        :param list_of_tables:
        :return: formatted conditions list
        """
        formatted_conditions: List[Dict[str, Tuple[str, ...]]] = []
        for list_of_conditions in multiple_lists_of_conditions:
            for column_name, value in list_of_conditions:
                for table in list_of_tables:
                    if column_name != table.name:
                        continue
                    column_index = table.find_column_index(column_name)
                    formatted_conditions.append(
                        {column_name: tuple(row) for row in table.data if row[column_index] == value})
        return formatted_conditions

    def __apply_aggregate(self, list_of_rows: List[List[str]],
                          column_aggregate_pair: List[Tuple[str, AggregateFunction]]) \
            -> Dict[str, float]:
        """
        Applying aggregate function to the rows that are passed as argument
        Columns on which are aggregate functions preformed are specified in the column_aggregate_pair
        :param list_of_rows:
        :param column_aggregate_pair:
        :return:
        """
        results = {}

        for column_name, func in column_aggregate_pair:
            col_index = self.find_column_index(column_name)
            column_data = [float(row[col_index]) for row in list_of_rows if self.__is_number(row[col_index])]
            aggregate_result = self.__apply_single_aggregate(column_data, func)
            results[column_name] = aggregate_result

        return results

    def __eligible_rows(self, multiple_lists_of_conditions: List[List[Tuple[str, str]]]) -> List[List[str]]:
        """
        Need to pass a list of pairs column:value in order for
        A method to select all rows that satisfy those conditions
        :param multiple_lists_of_conditions:
        :return:
        """
        list_of_rows: List[List[str]] = []
        for list_of_conditions in multiple_lists_of_conditions:
            for row in self.data:
                num_of_satisfactions = 0  # how many column values does a specific row satisfy
                for pair in list_of_conditions:
                    if row[self.find_column_index(pair[0])] == pair[1]:
                        num_of_satisfactions += 1
                if num_of_satisfactions == len(list_of_conditions):
                    list_of_rows.append(row)
        return list_of_rows

    def search_without_indexes(self, multiple_lists_of_conditions: List[List[Tuple[str, str]]],
                               column_aggregate_pair: List[Tuple[str, AggregateFunction]]) \
            -> Dict[str, List[List[Tuple[str, str]]] | Dict[str, float]]:
        """
        Going through multiple lists of conditions and merging all the rows
        And performing specified aggregate functions on specified columns
        :param multiple_lists_of_conditions:
        :param column_aggregate_pair:
        :return:
        """
        list_of_rows: List[List[str]] = self.__eligible_rows(multiple_lists_of_conditions)

        if len(list_of_rows) == 0:
            return {"conditions": multiple_lists_of_conditions, "results": {}}
        # removing duplicates
        unique_list_of_rows = [list(row) for row in set(tuple(row) for row in list_of_rows)]

        aggregates = self.__apply_aggregate(unique_list_of_rows, column_aggregate_pair)

        return {"conditions": multiple_lists_of_conditions, "results": aggregates}

    def __eligible_rows_bitmap(self, multiple_lists_of_conditions: List[List[Tuple[str, str]]]) -> List[List[str]]:
        """
        Need to pass a list of pairs column:value in order for
        A method to select all rows that satisfy those conditions
        :param multiple_lists_of_conditions:
        :return:
        """
        num_rows = len(self.data)
        combined_bitmap = [0] * num_rows

        # Combine bitmaps for different conditions
        for list_of_conditions in multiple_lists_of_conditions:
            # Initialize the bitmap result with all 1s (assuming all rows are eligible)
            bitmap_result = [1] * num_rows
            # Combine bitmaps for all conditions using AND
            for column_name, value in list_of_conditions:
                col_index = self.find_column_index(column_name)
                if value not in self._bitmap_indexes[col_index - 1]:
                    raise ValueError(f"This value is not referenced in the table {column_name}")
                value_bitmap = self._bitmap_indexes[col_index - 1][value]
                bitmap_result = [bitmap_result[i] & value_bitmap[i] for i in range(num_rows)]
            # Combine different "AND conditions" using OR
            combined_bitmap = [combined_bitmap[i] | bitmap_result[i] for i in range(num_rows)]

        # Get the rows that satisfy all conditions
        satisfying_rows = [self.data[i] for i in range(num_rows) if combined_bitmap[i] == 1]

        return satisfying_rows

    def search_with_bitmap(self, list_of_tables: List[Table],
                           multiple_lists_of_conditions: List[List[Tuple[str, str]]],
                           column_aggregate_pair: List[Tuple[str, AggregateFunction]]) \
            -> Dict[str, List[Dict[str, Tuple]] | Dict[str, float]]:
        """
        Going through multiple lists of conditions and merging all the rows
        And performing specified aggregate functions on specified columns
        :param list_of_tables:
        :param multiple_lists_of_conditions:
        :param column_aggregate_pair:
        :return:
        """
        list_of_rows: List[List[str]] = self.__eligible_rows_bitmap(multiple_lists_of_conditions)

        formatted_conditions = self.format_conditions(multiple_lists_of_conditions, list_of_tables)

        if len(list_of_rows) == 0:
            return {"indexed columns": formatted_conditions, "results": {}}

        # removing duplicates
        unique_list_of_rows = [list(row) for row in set(tuple(row) for row in list_of_rows)]

        aggregates = self.__apply_aggregate(unique_list_of_rows, column_aggregate_pair)

        return {"indexed columns": formatted_conditions, "results": aggregates}
