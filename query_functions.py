import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
from pathlib import Path
import numpy as np
import logging
import sys
from typing import Dict, List, Tuple
import tabulate

class DatabaseQueries:
    def __init__(self, db_params):
        self.db_params = db_params
        self.__setup_logging()

    def __setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('import.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def industrial_zone_businesses_main_act_query(self):
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
        columns = ["Name", "Activity Code", "Activity Description", "Industrial Zone"]
        return query, columns

    def count_businesses_in_zone(self):
        query = """
        SELECT industrial_zones.name, COUNT(*) 
        FROM general_businesses 
            JOIN industrial_zone_businesses  
                ON general_businesses.id = industrial_zone_businesses.business_id
            JOIN industrial_zones
                ON industrial_zones.id = industrial_zone_businesses.zone_id
        GROUP BY industrial_zones.name
        """
        columns = ["Industrial Zone", "Number of businesses"]
        return query, columns

    def return_query_result(self, option):
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        options = {
            0: self.industrial_zone_businesses_main_act_query,
            1: self.count_businesses_in_zone
        }
        if option == 1:
            params = input(
                "Enter industrial zone: "
            ).upper()
        try:
            query, columns = options[option]()
            cur.execute(query)
            df = pd.DataFrame(cur.fetchall())
            df.columns = columns
            output_path = Path.cwd()/"query_output.xlsx"
            df.to_excel(output_path, index=False)
            print(f"Files saved to {output_path}")
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
    option = input()
    return queryCarrier.return_query_result(int(option))

if __name__ == "__main__":
    print(main())