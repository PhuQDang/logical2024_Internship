import pandas as pd
import numpy as np
import re
import psycopg2
import logging
import sys
from pathlib import Path
import time

class QueryPrompter:

    COL_NAME = [
        "Registration Number", #0
        "Business Name", #1
        "Address", #2 
        "Authorized Capital", #3
        "Phone", #4
        "Email", #5
        "Legal Representative", #6
        "Main Activity", #7
        "All Activities", #8
        "Business Model", #9
        "Shareholders", #10
        "Domestic", #11
        "Industrial Zone", #12
        "Number of Businesses" #13
    ]

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

    def __get_industrial_parks(self):
        query = "SELECT name FROM industrial_parks ORDER BY name"
        res = self.query_data_raw(query, [])
        return res


    def verify_capital_input(self):
        while True:
            try:
                min_capital = input("Enter minimum authorization capital in VND (default = 0 VND): ")
                if min_capital.strip() == "":
                    return 0
                min_capital = int(min_capital.strip())
                if min_capital < 0:
                    print("Invalid value for minimum authorized capital. Please enter another number")
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
        cols = [self.COL_NAME[1], self.COL_NAME[3]]
        return (query, [min_capital], cols)
    
    def industrial_park_business_capital_query(self):
        rank = {
            0: "general_businesses.name",
            1: "general_businesses.auth_capital",
            2: "industrial_zones.name"
        }
        
        min_capital = self.verify_capital_input()
        
        query = """
        SELECT general_businesses.name as b_name,
               general_businesses.auth_capital as auth_capital,
               industrial_parks.name as park_name
        FROM general_businesses
            JOIN industrial_parks
                ON general_businesses.park_id = industrial_parks.id
        WHERE auth_capital >= %s
        """
        cols = [self.COL_NAME[1], self.COL_NAME[3], self.COL_NAME[12]]
        return (query, [min_capital], cols)
    
    def industrial_park_businesses_all_query(self):
        query = """
        SELECT general_businesses.name, business_act.act_code, activities.descr, industrial_parks.name  
        FROM general_businesses 
            JOIN industrial_parks
                ON general_businesses.park_id = industrial_parks.id
            JOIN business_act 
                ON business_act.business_id = general_businesses.id
            JOIN activities
                ON activities.code = business_act.act_code
        WHERE main_act = true;
        """
        cols = ["Name", "Activity Code", "Activity Description", "Industrial Zone"]
        return (query, [], cols)

    def industrial_park_businesses_count(self):
        query = """
        SELECT industrial_parks.name as zone_name, COUNT(*) as number_of_businesses
        FROM general_businesses
            JOIN industrial_parks
                ON general_businesses.park_id = industrial_parks.id
        GROUP BY industrial_parks.name
        ORDER BY number_of_businesses
        """
        cols = [self.COL_NAME[12], self.COL_NAME[13]]
        return (query, [], cols)

    def businesses_in_industrial_park(self):
        query = """
        SELECT general_businesses.name as b_name,
               general_businesses.address as addr
        FROM general_businesses
            JOIN industrial_parks
                ON industrial_parks.id = general_businesses.park_id
        WHERE industrial_parks.name LIKE %s
        """
        available_zones = list(map(lambda x: x[0], self.__get_industrial_parks()))
        for i, z_name in enumerate(available_zones):
            print(f"{i}. {z_name}")
        zone = '%' + available_zones[int(input("Enter a zone number: "))]
        cols = [self.COL_NAME[1], self.COL_NAME[2]]
        return (query, [zone], cols)

    def query_data_raw(self, query: str, query_params: list, **kwargs):
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()

        try:
            cur.execute(query, (*query_params,))
            data = cur.fetchall()
            self.logger.info("Finish returning query results")
            return data
        except Exception as e:
            conn.rollback()
            self.logger.error(str(e))
            raise
        finally:
            if conn:
                cur.close()
                conn.close()

    def query_results(self):
        query_options = {
            1: self.all_businesses_capital_query,
            2: self.industrial_park_businesses_all_query,
            3: self.businesses_in_industrial_park,
            4: self.industrial_park_business_capital_query,
            5: self.industrial_park_businesses_count
        }
        print("Query options:\n"
              "\t1. Businesses based on authorized capital\n"
              "\t2. Businesses in all industrial parks\n"
              "\t3. Businesses in a specified industrial park\n"
              "\t4. Businessed in industrial park filtered by authorized capital\n"
              "\t5. Number of businesses in industrial parks\n"
              "\t0. Quit")
        try:
            option = int(input("Enter which query to perform: "))
            if not option:
                sys.exit(0)
            query, query_params, columns = query_options[option]()
            data = self.query_data_raw(query, query_params)
            df = pd.DataFrame(data)
            df.columns = columns
            output_path = Path.cwd() / f"query_output_{time.time()}.xlsx"
            df.to_excel(output_path, index=False)
        except Exception as e:
            self.logger.error(str(e))
            raise
        return df

class PotentialCustomers(QueryPrompter):
    def __init__(self, db_params):
        super().__init__(db_params)
        self.__retrieve_raw_data()
    
    def __retrieve_raw_data(self):
        try:
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            cur.execute("""
            SELECT general_businesses.id AS business_id,
                general_businesses.name AS business_name,
                general_businesses.reg_number AS reg_number,
                general_businesses.auth_capital AS auth_capital,
                general_businesses.park_id AS park_id,
                business_act.act_code AS act_code
            FROM general_businesses
                JOIN business_act 
                    ON general_businesses.id = business_act.business_id
            WHERE general_businesses.park_id is not NULL
            """)
            df = pd.DataFrame(cur.fetchall())
            df.columns = ["business_id", "name", "reg_number", "auth_capital", "park_id", "act_code"]
            self.logger.info("Finished getting raw data")
            self.df = df
            return
        except Exception as e:
            conn.rollback()
            self.logger.error(str(e))
            raise
        finally:
            if conn:
                cur.close()
                conn.close()
    
    def classify(self, targetCost: int = 3e9):
        dfTemp = self.df
        actCode = [r'^162[\d]+', r'^20[\d]+', r'^22[\d]+', r'^24[\d]+', r'^25[\d]+', r'^26[\d]+', r'^27[\d]+']
        mask = (dfTemp['act_code'].apply(lambda x: np.array([re.search(code, x) != None for code in actCode]).any())) & (dfTemp['auth_capital'] > targetCost)
        dfCleaned = dfTemp[mask].dropna().drop_duplicates('business_id', keep='first')[['name', 'reg_number', 'auth_capital', 'park_id']].sort_values(by='auth_capital')
        return dfCleaned
    
    def export_to_(self, data, f_format: str|None ='csv' ) -> None:
        export_f = {
            'csv': pd.DataFrame.to_csv,
            'xlsx': pd.DataFrame.to_excel
        }
        try:
            out_path = Path.cwd() / f"customers_raw_.{f_format}"
            export_f[f_format](data, out_path, index=False)
        except Exception as e:
            self.logger.error(e)
            raise

def main():
    db_param = {
        'host': 'localhost',
        'database': 'businessesdb',
        'user': 'postgres',
        'password': '1234',
        'port': '5432'
    }
    try:
        custClass = PotentialCustomers(db_params=db_param)
        start = time.time()
        res = custClass.classify()
        custClass.export_to_(res)
        print(res)
        print("Runtime = {:.3g}s".format(time.time()-start))
    except Exception as e:
        print(str(e))

if __name__ == "__main__":
    main()