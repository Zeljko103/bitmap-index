import random
import time
from typing import List

from table import MainTable, Table, AggregateFunction


# - assumption that there are no more than 10k rows - everything is done in memory
# - for each indexed column there is an additional table with the same name as the column
# - search by equality conditions over one or more indexed columns and corresponding logical AND/OR operations
# - during search, you specify which aggregate function is applied to non-indexed columns
# - when printing aggregate function results, also print values of non-indexed columns from the corresponding indexed tables
# - the system also supports executing searches without indexes
# - meta schema and data are loaded from input files


def generate_data_file(file_path: str, num_rows: int):
    with open(file_path, 'w') as file:
        for i in range(1, num_rows + 1):
            col1 = i
            col2 = random.choice(['A', 'B'])
            col3 = random.choice(['X', 'Y', 'Z'])
            col4 = random.choice(['I', 'J', 'K', 'L'])
            col5 = random.randint(10, 100)
            col6 = random.randint(100, 1000)
            file.write(f"{col1},{col2},{col3},{col4},{col5},{col6}\n")
        file.write("\nA,Alfa1,Beta1,Gamma1\nB,Alfa2,Beta2,Gamma2\n\nX,Delta1\nY,Delta2\nZ,Delta3\n\nI,Epsilon1,Eta1\n"
                   "J,Epsilon2,Eta2\nK,Epsilon3,Eta3\nL,Epsilon4,Eta4\n\n")


def read_schema(file_path: str) -> List[Table]:
    with open(file_path, 'r') as file:
        lines = file.readlines()

    tables: List[Table] = []

    sundered_line = lines[0].split('(')

    fact_table_name = sundered_line[0]
    fact_table_columns = sundered_line[1].replace(')', '').strip().split(',')

    fact_table = MainTable(fact_table_name, fact_table_columns)

    tables.append(fact_table)

    for line in lines[1:]:
        sundered_line = line.split('(')
        table_name = sundered_line[0]
        table_columns = sundered_line[1].replace(')', '').strip().split(',')
        table = Table(table_name, table_columns)
        tables.append(table)

    return tables


def fill_tables(file_path: str, tables: List[Table]) -> List[Table]:
    with open(file_path, 'r') as file:
        lines = file.readlines()

    table_index: int = 0  # assumption that the same order will be in meta_schema.txt and data.txt
    table_data: List[List] = []

    for line in lines:
        if line == "\n":  # switch to the next table
            if table_index > 0:
                # making bitmap indexes
                column_index = tables[0].columns.index(tables[0].columns[table_index])
                unique_values = list(set(row[column_index] for row in tables[0].data))
                bitmap_index = {value: [0] * len(tables[0].data) for value in unique_values}

                for i, row in enumerate(tables[0].data):
                    value = row[column_index]
                    bitmap_index[value][i] = 1
                tables[0].list_of_bitmap_indexes.append(bitmap_index)

            tables[table_index].data = table_data
            table_data = []
            table_index += 1
            continue
        table_data.append(line.strip().split(','))

    return tables


if __name__ == "__main__":
    schema_tables = read_schema("schema_and_data/meta_schema.txt")

    filled_tables = fill_tables("schema_and_data/10000_elements.txt", schema_tables)

    main_table = filled_tables[0]

    conditions_and_only = [[("D1", "A"), ("D2", "X")]]
    tables_referenced_in_conditions = [
        table for condition in conditions_and_only[0]
        for table in filled_tables[1:]
        if table.name == condition[0]
    ]

    # test for search without indexes using AND condition only
    print(main_table.search_without_indexes(conditions_and_only, [("Fact1", AggregateFunction.AVG)]))

    # test for search with indexes using AND condition only
    print(main_table.search_with_bitmap(tables_referenced_in_conditions, conditions_and_only,
                                        [("Fact1", AggregateFunction.AVG)]))

    """
    Inner list consists of tuples column_name:value and are parsed as tuple1 AND tuple2
    Inner lists are looked upon as inner_list1 OR inner_list2
    """
    conditions = [[("D1", "A"), ("D2", "X")],
                  [("D1", "B"), ("D2", "Y")],
                  [("D2", "Y")]]

    tables_referenced_in_conditions = list({table for condition_group in conditions
                                            for column, _ in condition_group
                                            for table in filled_tables[1:]
                                            if table.name == column})

    start1 = time.perf_counter()
    # test for search without indexes using OR condition also
    print(main_table.search_without_indexes(conditions, [("Fact1", AggregateFunction.COUNT),
                                                         ("Fact2", AggregateFunction.SUM)]))
    end1 = time.perf_counter()
    elapsed_time1 = end1 - start1
    print(f"Search without indexes: {elapsed_time1:.6f}s")
    # test for search with indexes using OR condition also
    start2 = time.perf_counter()
    print(main_table.search_with_bitmap(tables_referenced_in_conditions, conditions,
                                        [("Fact1", AggregateFunction.COUNT),
                                         ("Fact2", AggregateFunction.SUM)]))
    end2 = time.perf_counter()
    elapsed_time2 = end2 - start2
    print(f"Search without indexes: {elapsed_time2:.6f}s")
    print(f"Time difference between search without and search with index is: {elapsed_time1 - elapsed_time2:.6f}s")
