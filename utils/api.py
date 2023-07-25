import requests
from typing import List

import pandas as pd
from numpy import dtype

from settings import CREATE_DB_API_URL, QUERY_API_URL


class ClientAPI:
    headers = {'content-type': 'application/json'}
    dtype_mappings = {
        'int64': 'INT',
        'float64': 'FLOAT'
    }


    @classmethod
    def create_db(cls, name: str):
        requests.get(
            CREATE_DB_API_URL, 
            params={'db_name': name}, 
            headers=cls.headers
        )
    
    
    @classmethod
    def insert_many(cls, db: str, table: str, df: pd.DataFrame):
        rows = df.to_records(index=False)
        print(f'INSERT INTO {table} VALUES {", ".join([str(r) for r in rows])}')
        
        r = requests.get(
            QUERY_API_URL,
            params={'db_name': db, 'query_str': f'INSERT INTO {table} VALUES {", ".join([str(r) for r in rows])}'},
            headers=cls.headers
        )
    

    @classmethod
    def select_all(cls, db: str, table: str):
        r = requests.get(
            QUERY_API_URL,
            params={'db_name': db, 'query_str': f'SELECT * FROM {table}'},
            headers=cls.headers
        )
        return r.json()
    

    @classmethod
    def create_table(cls, db: str, table: str, cols: List[str], dtypes: List[dtype]):
        query_str = f'''CREATE TABLE IF NOT EXISTS {table} ({", ".join([
            f"{column} {cls.dtype_mappings.get(str(dtype), 'TEXT')}" for (column, dtype) in zip(cols, dtypes)])
        });'''
        requests.get(
            QUERY_API_URL,
            params={'db_name': db, 'query_str': query_str},
            headers=cls.headers
        )