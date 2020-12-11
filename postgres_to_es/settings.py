import os
from dotenv import load_dotenv
import logging


# Load enviroment variables
basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env_local'))


# Constans
ES_URL = os.environ.get('ETL_ELASTICSEARCH_URL', '127.0.0.1:9200')
ES_INDEX_NAME = os.environ.get('ETL_ELASTICSEARCH_INDEX_NAME', 'movies')

## DB settings
LIMIT = int(os.environ.get('ETL_BATCH_LIMIT', 100))
DSN = {
    'dbname': os.environ.get('ETL_POSTGRES_DBNAME'),
    'user': os.environ.get('ETL_POSTGRES_USER'),
    'password': os.environ.get('ETL_POSTGRES_PASSWORD'),
    'host': os.environ.get('ETL_POSTGRES_HOST', 'localhost'),
    'port': os.environ.get('ETL_POSTGRES_PORT', 5432)
}

LOGGER_LEVEL = logging.DEBUG
