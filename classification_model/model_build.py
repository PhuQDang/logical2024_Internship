import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import warnings
import seaborn as sns
warnings.filterwarnings('ignore')
import psycopg2
import logging
import sys
import time
from typing import Dict, List
import re
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from collections import Counter
from pathlib import Path

class PotentialCustomersClassifier:
    def __init__(self, db_params):
        self.__db_params = db_params
        self.__setup_logging()
        self.retrieve_raw_data()

    def __setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('customer_finder.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def retrieve_raw_data(self) -> pd.DataFrame:
        try:
            conn = psycopg2.connect(**self.__db_params)
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
 

    def basic_classify(self, targetCost: int = 3e10) -> pd.DataFrame:
        dfTemp = self.df
        actCode = [r'^162[\d]+', r'^20[\d]+', r'^22[\d]+', r'^24[\d]+', r'^25[\d]+', r'^26[\d]+', r'^27[\d]+']
        mask = (dfTemp['act_code'].apply(lambda x: np.array([re.search(code, x) != None for code in actCode]).any())) & (dfTemp['auth_capital'] > targetCost)
        dfCleaned = dfTemp[mask].dropna().drop_duplicates('business_id', keep='first')[['name', 'reg_number', 'auth_capital', 'park_id']].sort_values(by='auth_capital')
        return dfCleaned

    def create_model_(self):
        ...

    def plot_data(self, df: pd.DataFrame):
        ...

def main():
    db_param = {
        'host': 'localhost',
        'database': 'businessesdb',
        'user': 'postgres',
        'password': '1234',
        'port': '5432'
    }
    try:
        custClass = PotentialCustomersClassifier(db_params=db_param)
        start = time.time()
        res = custClass.basic_classify()
        custClass.export_to_(res)
        print(res)
        print("Runtime = {:.3g}s".format(time.time()-start))
    except Exception as e:
        print(str(e))

if __name__ == "__main__":
    main()