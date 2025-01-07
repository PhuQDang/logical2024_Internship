import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
import numpy as np
import logging
import sys
from typing import Dict, List, Tuple
import re
import tabulate

class DatabaseQueries:
    def __init__(self, db_params):
        self.db_params = db_params
        pass

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('import.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def __industrial_zone_businesses_main_act_query(self):
        query = """
        SELECT general_businesses.name, business_act.act_code, activities.descr, industrial_zones.name  
        FROM general_businesses 
            JOIN industrial_zone_businesses  
                ON general_businesses.id = industrial_zone_businesses.business_id
            JOIN business_act 
                ON business_act.business_id = general_businesses.id
            JOIN activities
                ON activities.code = business_act.act_code
            JOIN industrial_zones
                ON industrial_zones.id = industrial_zone_businesses.zone_id
        WHERE main_act = true;
        """
        return query

    def return_query_result(self, option = None):
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()

        try:
            cur.execute(self.__industrial_zone_businesses_main_act_query())
            return tabulate.tabulate(cur.fetchall(), tablefmt="pipe")
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error executing query: {str(e)}")
            raise
        finally:
            if conn:
                cur.close()
                conn.close()

def main():
    db_params = {
        'host': 'localhost',
        'database': 'businessesdb',
        'user': 'postgres',
        'password': '1234',
        'port': '5432'
    }
    queryCarrier = DatabaseQueries(db_params)
    return queryCarrier.return_query_result()

if __name__ == "__main__":
    print(main())