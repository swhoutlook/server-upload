import sqlite3
from typing import Dict

import pandas as pd


TableData = Dict[str, pd.DataFrame]

class FileParser:

    @classmethod
    def parse_file(cls, file: str) -> TableData:
        f = cls.parse_txt  if file.endswith('.txt')  else \
            cls.parse_csv  if file.endswith('.csv')  else \
            cls.parse_xlsx if file.endswith('.xlsx') else \
            cls.parse_db   if file.endswith('.db')   else None
        if not f:
            raise ValueError('Invalid file extension.')
        return f(file)
    

    @classmethod
    def parse_txt(cls, file: str) -> TableData:
        '''
        Each .txt file can only represent 1 table, so we use a default table name: "table"

        Required .txt file content format:

        <col_1_name>;<col_2_name>; ... <col_n_name>
        <val_11>;<val_12>, ... <val_1n>
        ...
        <val_n1>;<val_n2>, ... <val_nn>
        '''
        with open(file, 'r') as f:
            records = []
            cols = list(map(lambda col:col.strip(), f.readline().strip().split(';')))
            line = True
            while line:
                line = f.readline().strip()
                if line:
                    records.append(list(map(lambda val:val.strip(), line.split(';'))))

        return {'table': pd.DataFrame.from_records(records, columns=cols)}
    

    @classmethod
    def parse_csv(cls, file: str) -> TableData:
        '''
        Read .csv file and parse content into TableData format.
        Each .csv file can only represent 1 table, so we use a default table name: "table"

        CSV files must have a header row.
        '''
        return {'table': pd.read_csv(file)}
    

    @classmethod
    def parse_xlsx(cls, file: str) -> TableData:
        '''
        Read .xlsx file and parse content into TableData format.

        Each Excel worksheet must have a header row.
        '''
        workbk = pd.read_excel(file, sheet_name=None)
        return {sheet: workbk[sheet] for sheet in workbk}
    

    @classmethod
    def parse_db(cls, file: str) -> TableData:
        '''
        Read .db file (sqlite) and parse content into TableData format.
        '''
        res = {}
        conn = sqlite3.connect(file)
        cursor = conn.cursor()
        cursor.execute('SELECT name FROM sqlite_master WHERE type="table"')
        tables = cursor.fetchall()
        for table, in tables:            
            cursor.execute(f'SELECT * FROM {table}')
            cols = list(map(lambda x: x[0], cursor.description))
            rows = cursor.fetchall()
            res[table] = pd.DataFrame.from_records(rows, columns=cols)
        return res
        