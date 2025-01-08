import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
import numpy as np
import logging
import sys
from pathlib import Path

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

    def query_results(self):
        query_options = {
            1: self.all_businesses_capital_query,
            2: self.industrial_zone_business_capital_query,
        }
        option = input("Enter which query to perform: ")
        query, query_params, columns = query_options[option]()
        data = self.query_data_raw(query, query_params, columns)
        df = pd.DataFrame(data)
        df.columns = columns
        output_path = Path.cwd() / "query_output.xlsx"
        df.to_excel(output_path, index=False)
        return df

    def verify_capital_input(self):
        while True:
            try:
                min_capital = input("Enter minimum authorization capital in VND (default = 0 VND): ")
                if min_capital == "":
                    return 0
                min_capital = int(min_capital)
                if min_capital < 0:
                    continue
                return min_capital
            except ValueError:
                print("Invalid value, please enter a number")
    
    def all_businesses_capital_query(self) -> tuple[str, list, list]:
        min_capital = self.verify_capital_input()
        query = """
        SELECT general_businesses.name, 
               general_businesses.auth_capital
        FROM general_businesses
            WHERE auth_capital > %s
        ORDER BY auth_capital
        """
        cols = ["Business Name", "Authorized Capital"]
        return (query, [min_capital], cols)
    
    def industrial_zone_business_capital_query(self):
        rank = {
            0: "b_name",
            1: "auth_capital",
            2: "z_name"
        }
        
        min_capital = self.verify_capital_input()
        ranking_category = rank[int(input("Enter ranking category: "))]
        
        query = """
        SELECT general_businesses.name as b_name,
               general_businesses.auth_capital as auth_capital,
               industrial_zones.name as z_name
        FROM general_businesses
            JOIN industrial_zone_businesses
                ON general_businesses.id = industrial_zone_businesses.business_id
            JOIN industrial_zones
                ON industrial_zones.id = industrial_zone_businesses.zone_id
        WHERE auth_capital >= %s
        ORDER BY %s
        """
        cols = ["Business Name", "Authorized Capital", "Industrial Zone"]
        return (query, [min_capital, ranking_category], cols)
    
    def industrial_zone_businesses_all_query(self):
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
        cols = ["Name", "Activity Code", "Activity Description", "Industrial Zone"]
        return (query, [], cols)

    def industrial_zone_businesses_count(self):
        query = """
        SELECT industrial_zones.name as zone_name, COUNT(*) as number_of_businesses
        FROM general_businesses 
            JOIN industrial_zone_businesses  
                ON general_businesses.id = industrial_zone_businesses.business_id
            JOIN industrial_zones
                ON industrial_zones.id = industrial_zone_businesses.zone_id
        GROUP BY industrial_zones.name
        ORDER BY number_of_businesses
        """
        cols = ["Industrial Zone", "Number of businesses"]
        return (query, [], cols)

    def query_data_raw(self, query: str, query_params: list, **kwargs):
        conn = psycopg2.connect()
        cur = conn.cursor()

        try:
            cur.execute(query, (*query_params,))
            data = cur.fetchall()
            if data == None:
                return None
            return data
        except Exception as e:
            conn.rollback()
            self.logger.error(str(e))
            raise
        finally:
            if conn:
                cur.close()
                conn.close()

def main():
    ...

if __name__ == "__main__":
    main()