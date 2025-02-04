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
                business_act.act_code AS act_code,
                general_businesses.auth_capital AS auth_capital
            FROM general_businesses
                JOIN industrial_parks 
                    ON general_businesses.park_id = industrial_parks.id
                JOIN business_act 
                    ON general_businesses.id = business_act.business_id
            """)
            df = pd.DataFrame(cur.fetchall())
            df.columns = ["business_id", "act_code", "act_descr", "auth_cap"]
            self.logger.info("Finished getting raw data")
            return df
        except Exception as e:
            conn.rollback()
            self.logger.error(str(e))
            raise
        finally:
            if conn:
                cur.close()
                conn.close()
    
    def export_to_(self, f_format: str|None ='csv' ) -> None:
        export_f = {
            'csv': pd.DataFrame.to_csv,
            'xlsx': pd.DataFrame.to_excel
        }
        try:
            raw_data = self.retrieve_raw_data()
            out_path = Path.cwd() / f"customers_raw_.{f_format}"
            export_f[f_format](raw_data, out_path, index=False)
        except Exception as e:
            self.logger.error(e)
            raise
    
    def basic_classify(self, targetCost:int|None = 3e10) -> pd.DataFrame:
        targetCost = 3e10
        actCode = ['16%', '19%', '20%', '21%', '22%', '23%', '24%', '25%', '26%', '27%']
        res = []
        try:
            conn = psycopg2.connect(**self.__db_params)
            cur = conn.cursor()
            for code in actCode:
                cur.execute("""
                    SELECT DISTINCT general_businesses.name as business_name,
                           general_businesses.reg_number as registration_number
                    FROM general_businesses 
                        JOIN business_act ON general_businesses.id = business_act.business_id
                    WHERE auth_capital > %s AND general_businesses.park_id is not NULL
                            AND business_act.act_code like %s
                """, (targetCost, code,))
                res += cur.fetchall()
            df = pd.DataFrame(res)
            df.columns = ["Name", "Registration Number"]
            df.drop_duplicates("Name")
            return df
        except Exception as e:
            conn.rollback()
            self.logger.error(str(e))
            raise
        finally:
            if conn:
                cur.close()
                conn.close()
            
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
        res = custClass.basic_classify()
        print(res)
    except Exception as e:
        print(str(e))

if __name__ == "__main__":
    main()