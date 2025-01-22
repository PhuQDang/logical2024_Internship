import pandas as pd
import psycopg2
import logging
import sys
from typing import Dict, List
import re
from industrial_park_classifier import IndustrialParkClassifier
import numpy as np
from rapidfuzz.fuzz import ratio, partial_ratio

class VNBusinessImporter:
    def __init__(self, db_params: Dict[str, str], excel_file: str):
        self.db_params = db_params
        self.excel_file = excel_file
        self.classifier = IndustrialParkClassifier(self.db_params)
        self.setup_logging()
        self.test_array = []
        
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

    def create_schema(self):
        """Create database schema"""
        try:
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()

            schema_sql = """
            CREATE TABLE IF NOT EXISTS park_placement(
                park_id int REFERENCES industrial_parks(id),
                div_id  varchar(20) REFERENCES areas(code),
                PRIMARY KEY(park_id, div_id)
            );

            CREATE TABLE IF NOT EXISTS activities (
                code varchar(5) PRIMARY KEY,
                descr varchar(255)
            );

            CREATE TABLE IF NOT EXISTS business_type(
                id serial PRIMARY KEY,
                descr varchar(100) UNIQUE
            );


            CREATE TABLE IF NOT EXISTS general_businesses(
                id serial PRIMARY KEY,
                name varchar(255),
                reg_number varchar(20),
                address varchar(255),
                area_id varchar(20) REFERENCES areas(code),
                park_id int REFERENCES industrial_parks(id),
                phone varchar(20)[],
                email varchar(100),
                auth_capital bigint,
                type_id int REFERENCES business_type(id),
                domestic boolean
            );

            CREATE TABLE IF NOT EXISTS legal_rep(
                business_id int REFERENCES general_businesses(id),
                name varchar(100),
                PRIMARY KEY (business_id, name)
            );

            CREATE TABLE IF NOT EXISTS business_act(
                business_id int REFERENCES general_businesses(id),
                act_code varchar(5) REFERENCES activities(code),
                main_act boolean,
                PRIMARY KEY(business_id, act_code)
            );

            CREATE TABLE IF NOT EXISTS shareholders(
                id  serial PRIMARY KEY,
                name varchar(80)
            );

            CREATE TABLE IF NOT EXISTS business_shareholder(
                business_id int REFERENCES general_businesses(id),
                shareholder_id int REFERENCES shareholders(id),
                type varchar(50),
                PRIMARY KEY (business_id, shareholder_id)
            );
            """
            
            cur.execute(schema_sql)
            conn.commit()
            self.logger.info("Schema created successfully")

        except Exception as e:
            self.logger.error(f"Error creating schema: {str(e)}")
            raise
        finally:
            if conn:
                cur.close()
                conn.close()

    def process_phone_numbers(self, phone_str: str) -> List[int]:
        """Convert phone string to array of integers"""
        if pd.isna(phone_str):
            return []
        # Extract numbers only
        phones = re.findall(r'\d+[^-,;/]*', phone_str.replace('–', '-'))
        return [phone.strip() for phone in phones]

    def __fetch_divisions(self):
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        
        try:
            cur.execute("SELECT areas.code, areas.name FROM areas")
            self.areas_map = pd.DataFrame(cur.fetchall())
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error fetching divisions: {str(e)}")
        finally:
            cur.close()
            conn.close()
        
    def __fetch_area_code(self, address, parent='NULL'):
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        
        try:
            cur.execute("SELECT areas.code, areas.name FROM areas WHERE parent_code = %s", (parent,))
            areas_map = pd.DataFrame(cur.fetchall())
            res, _ = max(areas_map.iloc(), key=lambda x: partial_ratio(x[1], address))
            return res
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error fetching divisions: {str(e)}")
        finally:
            cur.close()
            conn.close()


    def process_admin_divisions(self, row) -> Dict[str, int]:
        """Process district and ward data into admin_divisions table"""
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        self.test_array
        last_access_ = None
        get_id = None
        try:
            if pd.notna(row['province']):
                province_temp = re.search(r"(?<=Tỉnh).+|(?<=tỉnh).+", row['province']).group().strip()
                cur.execute("""
                    SELECT areas.code
                    FROM areas
                        WHERE parent_code IS NULL AND areas.name LIKE %s             
                """, ('%'+province_temp+'%',))
                _province_id = cur.fetchone()[0]
                last_access_ = province_temp
                get_id = _province_id
                if pd.notna(row['district']):
                    district_temp = re.search(r"(?<=Thành phố).+|(?<=Huyện).+|(?<=Thị Xã).+|(?<=Thị xã).+|(?<=TX).+|(?<=tx).+", row['district']).group().strip()
                    cur.execute("""
                        SELECT areas.code
                        FROM areas
                            WHERE parent_code = %s AND areas.name LIKE %s            
                    """, (_province_id, '%'+district_temp+'%'))
                    _district_id = cur.fetchone()
                    if _district_id is None:
                        _district_id = self.__fetch_area_code(district_temp, parent=_province_id)
                    else: 
                        _district_id = _district_id[0]
                    last_access_ = district_temp
                    get_id = _district_id
                    if pd.notna(row['ward']):
                        ward_temp = re.search(r"(?<=Phường).+|(?<=Xã).+|(?<=Thị Trấn).+|(?<=Thị trấn).+|(?<=Tt).+|(?<=tt)", row['ward']).group().strip()
                        cur.execute("""
                            SELECT areas.code 
                            FROM areas
                                WHERE parent_code = %s AND areas.name LIKE %s
                        """, (_district_id, '%'+ward_temp+'%'))
                        _ward_id = cur.fetchone()
                        if _ward_id is None:
                            _ward_id = self.__fetch_area_code(ward_temp, parent=_district_id)
                        else:
                            _ward_id = _ward_id[0]
                        last_access_ = ward_temp
                        get_id = _ward_id
                #         return _ward_id
                #     return _district_id
                # return _province_id
                self.test_array.append([last_access_, get_id])
                return get_id
        except Exception as e:
            self.logger.error(e)
            raise
        finally:
            cur.close()
            conn.close()


    def process_business_types(self, df: pd.DataFrame) -> Dict[str, int]:
        """Process business types"""
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        type_map = {}

        try:
            business_types = df['model'].unique()
            for btype in business_types:
                if pd.notna(btype):
                    cur.execute(
                        """
                        WITH e AS (
                            INSERT INTO business_type (descr) 
                                VALUES (%s) 
                            ON CONFLICT DO NOTHING
                            RETURNING id
                        )
                        SELECT * FROM e
                        UNION SELECT id FROM business_type WHERE descr = %s
                        """, (btype,btype)
                    )
                    type_id = cur.fetchone()[0]
                    type_map[btype] = type_id

            conn.commit()
            return type_map

        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error processing business types: {str(e)}")
            raise
        finally:
            cur.close()
            conn.close()
            print("Import business types complete")

    def process_activities(self, df: pd.DataFrame) -> Dict[str, int]:
        """Process business activities"""
        # This is a simplified version - you might need to adjust based on your actual activity codes
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        activity_map = {}

        try:
            # Process both main and additional activities
            all_activities = {}
            
            # Add main activities
            main_activities = list(map(lambda y: list(map(lambda x: x.removesuffix(',').split(':'), re.findall(r"[\d]{4,}:[\D]*", str(y)))), df['main_act'].dropna()))
            for i in range(len(main_activities)):
                if len(main_activities[i]) < 2:
                    continue
                all_activities[main_activities[i][0]] = main_activities[i][1]
            # Add other activities
            other_activities = list(map(lambda y: list(map(lambda x: x.removesuffix(',').split(':'), re.findall(r"[\d]{4,}:[\D]*", str(y)))), df['all_act'].dropna()))
            for i in range(len(other_activities)):
                for j in range(len(other_activities[i])):
                    if len(other_activities[i][j]) < 2:
                        continue
                    all_activities[other_activities[i][j][0]] = other_activities[i][j][1]
            # Insert activities and generate codes
            for code, activity in all_activities.items():
                cur.execute(
                    """
                    WITH e AS (
                        INSERT INTO activities (code, descr) 
                            VALUES (%s, %s) 
                        ON CONFLICT DO NOTHING 
                        RETURNING code
                    )
                    SELECT * FROM e 
                    UNION 
                        SELECT code FROM activities WHERE code = %s
                    """, (code, activity, code)
                )
                activity_map[code] = activity
            conn.commit()
            print("Import activities complete")
            return activity_map

        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error processing activities: {str(e)}")
            raise
        finally:
            cur.close()
            conn.close()

    def process_shareholders(self, df: pd.DataFrame):
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        shareholders_map = {}
        try:
            shareholders = np.concatenate((df['co_fund'].dropna().unique(), df['shareholders'].dropna().unique()), axis=0)
            for s_list in shareholders:
                for _s in s_list.split(','):
                    _s = _s.strip().lower()
                    cur.execute("""
                        WITH e AS (
                            INSERT INTO shareholders (name)
                            VALUES
                                (%s)
                            ON CONFLICT DO NOTHING
                            RETURNING id
                        ) SELECT * FROM e
                        UNION SELECT id FROM shareholders WHERE name = %s
                    """, (_s, _s))
                    _id = cur.fetchone()[0]
                    shareholders_map[_s] = _id
            conn.commit()
            self.logger.info("Finish processing shareholders")
            return shareholders_map
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error processing shareholders: {str(e)}")
            raise
        finally:
            cur.close()
            conn.close()

    def import_data(self):
        """Main import process"""
        try:
            # Create schema
            self.create_schema()

            # Read Excel file
            df = pd.read_excel(self.excel_file)
            
            # Process reference data first
            type_map = self.process_business_types(df)
            activity_map = self.process_activities(df)
            shareholder_map = self.process_shareholders(df)

            # Process businesses
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()

            try:
                for _, row in df.iterrows():
                    # Insert business
                    area_key = f"{row['district']}|{row['ward']}"
                    area_id = self.process_admin_divisions(row)
                    if pd.notna(row['address']):
                        _park_id = self.classifier.classify_(row['address'])
                    else:
                        _park_id = None

                    if _park_id != None:
                        cur.execute("""
                            INSERT INTO park_placement (park_id, div_id)
                            VALUES
                                    (%s, %s)
                            ON CONFLICT DO NOTHING
                        """, (_park_id, area_id,))
                    
                    cur.execute("""
                        INSERT INTO general_businesses (
                            name, reg_number, address, area_id, park_id, phone, email,
                            auth_capital, type_id, domestic
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        row['business_name'],
                        row['reg_number'],
                        row['address'],
                        area_id,
                        _park_id,
                        self.process_phone_numbers(row['phone']),
                        row['email'],
                        row['auth_cap'],
                        type_map.get(row['model']),
                        row.get('domestic') == 'TN',
                    ))
                    
                    business_id = cur.fetchone()[0]
                    
                    # Insert business activities
                    if pd.notna(row['main_act']):
                        if row['main_act'] == "--":
                            pass
                        else:
                            main_act_code, _ = row['main_act'].split(':')
                            if main_act_code:
                                cur.execute("""
                                    INSERT INTO business_act (business_id, act_code, main_act)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT DO NOTHING
                                """, (business_id, main_act_code, True))

                    # Process other activities
                    if pd.notna(row['all_act']):
                        other_acts =list(map(lambda x: x.removesuffix(',').split(':'), re.findall(r"[\d]{4,}:[\D]*", row['all_act'])))
                        for act_kv in other_acts:
                            act_code, _ = act_kv
                            if act_code:
                                cur.execute("""
                                    INSERT INTO business_act (business_id, act_code, main_act)
                                    VALUES (%s, %s, %s)
                                    ON CONFLICT (business_id, act_code) DO NOTHING
                                """, (business_id, act_code, False))

                    # Process shareholders
                    for shareholder_list in ['co_fund', 'shareholders']:
                        if pd.notna(row.get(shareholder_list)):
                            shareholders = row[shareholder_list].split(',')
                            for s in shareholders:
                                s = s.strip().lower()
                                if s:
                                    cur.execute("""
                                        INSERT INTO business_shareholder (business_id, shareholder_id, type)
                                        VALUES (%s, %s, %s)
                                        ON CONFLICT DO NOTHING
                                    """, (business_id, shareholder_map[s], shareholder_list))

                    if pd.notna(row.get('legal_rep')):
                        rep = row['legal_rep']
                        cur.execute(
                            "INSERT INTO legal_rep (business_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            (business_id, rep,)
                        )
                        
                    

                conn.commit()
                self.logger.info(f"Successfully imported all data")

            except Exception as e:
                conn.rollback()
                raise e
            finally:
                cur.close()
                conn.close()

        except Exception as e:
            print(e)
            self.logger.error(f"Error in import process: {e}")
            raise

def main(fname):
    # Database connection parameters
    db_params = {
        'host': 'localhost',
        'database': 'divisiondb_test',
        'user': 'postgres',
        'password': '1234',
        'port': '5432'
    }
    
    # Excel file path
    excel_file = fname
    
    # Create importer and run import
    importer = VNBusinessImporter(db_params, excel_file)
    
    try:
        importer.import_data()
        print("Data import completed successfully!")
        for i, k in importer.test_array:
            if k == '01':
                print(i, k)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    fname = 'dsdn_1997_2024_processed.xlsx' #should create a dropbox to drop file in (front-end)
    main(fname)