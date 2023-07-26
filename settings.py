import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())


QUERY_API_URL=os.environ.get('QUERY_API_URL')
CREATE_DB_API_URL=os.environ.get('CREATE_DB_API_URL')
DEFAULT_TABLE_NAME=os.environ.get('DEFAULT_TABLE_NAME')
POSTGRES_USER=os.environ.get('POSTGRES_USER')
POSTGRES_PASSWORD=os.environ.get('POSTGRES_PASSWORD')
POSTGRES_HOST=os.environ.get('POSTGRES_HOST')
POSTGRES_PORT=os.environ.get('POSTGRES_PORT')
