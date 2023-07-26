import requests
from typing import List

import pandas as pd
import psycopg2
from numpy import dtype

from settings import (
    CREATE_DB_API_URL, 
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_HOST,
    POSTGRES_PORT
)

dtype_mappings = {
    'int64': 'INT',
    'float64': 'FLOAT'
}

class ClientAPI:
    headers = {'content-type': 'application/json'}
    
    @classmethod
    def create_conn(cls, db: str): 
        connection = psycopg2.connect(
            database=f'_{db}',
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        connection.autocommit = True
        return connection


    @classmethod
    def create_db(cls, db: str):
        r = requests.get(
            CREATE_DB_API_URL, 
            params={'db_name': '_' + db}, 
            headers=cls.headers
        )
        print(r.text)
    
    
    @classmethod
    def insert_many(cls, db: str, table: str, df: pd.DataFrame):
        rows = df.to_records(index=False)
        row_strs = ''
        for row in rows:
            row_str = '('
            for col in row:
                if not col:
                    row_str += 'NULL,'
                elif type(col) == str:
                    col = col.replace('\'', '\'\'').replace('(', '\\(').replace(')', '\\)')
                    row_str += f'\'{col}\','
                else:
                    row_str += f'{col},'
            row_strs  += f'{row_str[:-1]}),'

        query = f'INSERT INTO {table} VALUES {row_strs[:-1]}'
        conn = cls.create_conn(db)
        conn.cursor().execute(query)
        conn.commit()


    @classmethod
    def select_all(cls, db: str, table: str):
        query = f'SELECT * FROM {table.lower()}'
        conn = cls.create_conn(db)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        return cursor.fetchall()
    

    @classmethod
    def create_table(cls, db: str, table: str, cols: List[str], dtypes: List[dtype]):
        query = f'''CREATE TABLE IF NOT EXISTS {table} ({", ".join([
            f"{column} {dtype_mappings.get(str(dtype), 'TEXT')}" for (column, dtype) in zip(cols, dtypes)])
        });'''
        conn = cls.create_conn(db)
        conn.cursor().execute(query)
        conn.commit()
