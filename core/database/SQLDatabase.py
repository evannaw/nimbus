from core.database.DatabaseConn import DatabaseConn
import logging
import pymysql
from sqlite3 import Error
from sqlalchemy.types import NVARCHAR
import pandas as pd
import sys
pd.set_option('display.width',1000, 'display.max_columns',1000)
logger = logging.getLogger(__name__)

class SQLDatabase(DatabaseConn):
    def __init__(self, url):
        self.url = url
        self.conn = None

    def connect(self):
        try:
            self.conn = pymysql.connect(host='localhost',
                             user='root',
                             password='My_SQL_root_1',
                             db=url,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
            return self.conn

            logger.info(f'Successfully connected to sqllite database located at {self.url}')
        except Error as e:
            logger.error(f'Could not connect to sqlite datbase with exception: {e}')

    def select_as_dataframe(self, query:str):
        try:
            data = self.conn.execute(query)
            col_names = list(map(lambda x: x[0], data.description))
            return pd.DataFrame(data.fetchall(), columns=col_names)
        except Exception as e:
            logger.error(f'Could not query with exception: {e}')

    def insert_from_dataframe(self, sql_table, df:pd.DataFrame):
        if isinstance(df, pd.DataFrame):
            df = df.astype(str)
            df.astype(str).to_sql(sql_table, self.conn, if_exists='append', index=False, chunksize=100)
            logger.info(f'Successfully inserted {df.shape[0]} records\n{df.tail(5)}')

    def delete_record(self, query):
        try:
            logger.info(f"Query: {query}")
            logger.info(f'Deleted {self.conn.execute(query).rowcount} records from database')
            self.conn.commit()
        except Exception as e:
            logger.error(f'Could not delete records, exception: {e}')

    def disconnect(self):
        self.conn.close()
        logger.info('Disconected from sqllite database')