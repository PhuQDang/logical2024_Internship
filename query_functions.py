import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
import numpy as np
import logging
import sys
from typing import Dict, List, Tuple

class QueryPrompter:
    def __init__(self, db_params):
        self.db_params = db_params
        self.__setup_logging()
    
    def __setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('query.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def choose_query(self):
        options = {
                        
        }
        while True:
            try:
                option = input("Choose your query: \n")
                query = ...
                query_params = ...
                return self.__return_query_results(query, query_params)
            except Exception as e:
                self.logger.error(str(e))
                raise

    def __return_query_results(self, query, query_params, **kwargs):
        conn = psycopg2.connect()
        cur = conn.cursor()

        try:
            cur.execute(query, (query_params,))
            return cur.fetchall()
        except Exception as e:
            conn.rollback()
            self.logger.error(str(e))
            raise
        finally:
            if conn:
                cur.close()
                conn.close()

def main():
    pass
if __name__ == "__main__":
    main()