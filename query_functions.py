import pandas as pd
import psycopg2
import logging
import sys
from pathlib import Path

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
        "Industrial Zone" #12
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

    def __get_industrial_zones(self):
        query = "SELECT name FROM industrial_zones ORDER BY name"
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
    
    def industrial_zone_business_capital_query(self):
        rank = {
            0: "b_name",
            1: "auth_capital",
            2: "z_name"
        }
        
        min_capital = self.verify_capital_input()
        while True:
            try:
                print("Ranking categories:\n"
                    "0. Business name\n"
                    "1. Authorized capital\n"
                    "2. Industrial zone")
                ranking_category = rank[int(input("Enter ranking category: "))]
                if not (0 <= ranking_category <= 2):
                    print("Invalid value.")
                    continue
                break
            except ValueError:
                print("Invalid value.")
                continue
        
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
        cols = [self.COL_NAME[1], self.COL_NAME[3], self.COL_NAME[12]]
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
        cols = [self.COL_NAME[12], self.COL_NAME[13]]
        return (query, [], cols)

    def businesses_in_industrial_zone(self):
        query = """
        SELECT general_businesses.name as b_name,
               general_businesses.address as addr
        FROM general_businesses
            JOIN industrial_zone_businesses
                ON industrial_zone_businesses.business_id = general_businesses.id
            JOIN industrial_zones
                ON industrial_zones.id = industrial_zone_businesses.zone_id
        WHERE industrial_zones.name LIKE %s
        """
        available_zones = list(map(lambda x: x[0], self.__get_industrial_zones()))
        for i, z_name in enumerate(available_zones):
            print(f"{i}. {z_name}")
        zone = '%' + available_zones[int(input("Enter a zone number: "))] + '%'
        cols = [self.COL_NAME[1], self.COL_NAME[2]]
        return (query, [zone], cols)

    def query_data_raw(self, query: str, query_params: list, **kwargs):
        conn = psycopg2.connect(**self.db_params)
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
            self.logger.info("Finish returning query results")
            if conn:
                cur.close()
                conn.close()

    def query_results(self):
        query_options = {
            1: self.all_businesses_capital_query,
            2: self.businesses_in_industrial_zone,
            3: self.industrial_zone_businesses_all_query,
            4: self.industrial_zone_business_capital_query,
            5: self.industrial_zone_businesses_count
        }
        try:
            option = int(input("Enter which query to perform: "))
            query, query_params, columns = query_options[option]()
            data = self.query_data_raw(query, query_params)
            df = pd.DataFrame(data)
            df.columns = columns
            output_path = Path.cwd() / "query_output.xlsx"
            df.to_excel(output_path, index=False)
        except Exception as e:
            self.logger.error(str(e))
            raise
        return df

def main():
    db_params = {
        'host': 'localhost',
        'database': 'businessesdb',
        'user': 'postgres',
        'password': '1234',
        'port': '5432'
    }

    queryPrompter = QueryPrompter(db_params)
    while True:
        try:
            queryPrompter.query_results()
        except KeyboardInterrupt:
            sys.exit(0)
        except Exception as e:
            print(str(e))
            sys.exit(1)

if __name__ == "__main__":
    main()