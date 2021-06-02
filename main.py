import logging
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd

INPUT = 'input'
DEFINITIONS = 'definitions'
EXPORT = 'export'
OUTPUT = 'output'


def init_folders(path_: Path):
    if not os.path.exists(path_):
        os.makedirs(path_, mode=0o777)

    def _(sub_folder: str):
        sub_folder_path = Path(path_, sub_folder)

        if not os.path.exists(sub_folder_path):
            os.mkdir(sub_folder_path)

    for file in [DEFINITIONS, INPUT, EXPORT, OUTPUT]:
        _(file)


def extract_input_files(con, path_: Path):
    def read_file(file_: str):
        df = pd.read_csv(filepath_or_buffer=Path(path_, file_))
        table_name = os.path.splitext(file_)[0]
        con.register(table_name, df)

    for file in os.listdir(path_):
        read_file(file)


def execute_sql_definitions(con, path_: Path):
    if not os.path.exists(path_):
        return

    def execute(file_: str):
        file_path = Path(path_, file_)
        with open(file_path, 'r') as f:
            content = f.read()
            con.execute(content)

    for file in os.listdir(path_):
        execute(file)


def export_sql_outputs(con, path_: Path, output_path: Path):
    def execute(file_: str):
        file_path = Path(path_, file_)
        with open(file_path, 'r') as f:
            content = f.read()
            output_file_name = f'{os.path.splitext(file)[0]}.csv'
            con.execute(content).fetchdf().to_csv(Path(output_path, output_file_name), index=False)

    for file in os.listdir(path_):
        execute(file)


def validate_folder_is_not_empty(path_: Path):
    empty = len(os.listdir(path_)) == 0

    if empty:
        logging.warning(f'The folder "{path_}" is empty!')

    return not empty


def main():
    if len(sys.argv) < 2:
        logging.error('Please set the worker folder path for the tool')
        return

    path = Path(sys.argv[1])
    init_folders(path)

    export_folder_path = Path(path, EXPORT)
    input_folder_path = Path(path, INPUT)

    if not validate_folder_is_not_empty(export_folder_path):
        return

    if not validate_folder_is_not_empty(input_folder_path):
        return

    con = duckdb.connect(database=':memory:', read_only=False)

    extract_input_files(con, Path(path, INPUT))
    execute_sql_definitions(con, Path(path, DEFINITIONS))
    export_sql_outputs(con, Path(path, EXPORT), Path(path, OUTPUT))

    print("The process completed successfully!")


if __name__ == "__main__":
    main()
