import pandas as pd
import psycopg2
import logging
import sys
import re
from rapidfuzz.fuzz import ratio, partial_ratio

class IndustrialZoneBusinessesFinder:
    def __init__(self, db_params: dict[str, str], excel_file = None) -> None:
        self.db_params = db_params
        self.excel_file = excel_file
        self.setup_logging()
    
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('industrial_filter.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def create_industrial_schema(self) -> None:
        try:
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()

            schema_sql = """
            CREATE TABLE IF NOT EXISTS industrial_parks (
                id      SERIAL PRIMARY KEY,
                name    varchar(255) UNIQUE,
                area_id     int,
                UNIQUE (name, area_id)
            );
            CREATE TABLE IF NOT EXISTS industrial_zone_businesses (
                business_id     int REFERENCES general_businesses(id),
                zone_id         int REFERENCES industrial_parks(id),
                PRIMARY KEY (business_id, zone_id)
            );
            """
            cur.execute(schema_sql)
            conn.commit()
            self.logger.info("Schema created successfully")
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error creating schema: {str(e)}")
            raise
        finally:
            if conn:
                cur.close()
                conn.close()

    def process_industrial_parks(self) -> dict[int, str]:
        """
        Extract unique industrial zones and create a map of zone's name and zone's id
        """
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()

        cur.execute("SELECT id, address, area_id FROM general_businesses")
        id_addresses = cur.fetchall()
        industrialZoneMap = {}
        idZoneMap = {}

        def extract_industrial_zone(address: str) -> str:
            addressProcessed = re.search(r"(?<=KCN )[\D\d]+?,|(?<=Khu công nghiệp )[\D\d]+?,|(?<=Khu Công Nghiệp )[\D\d]+?,|(?<=khu công nghiệp )[\D\d]+?,", address)
            if addressProcessed != None:
                addressProcessed = re.findall(r"[^–()-]+", addressProcessed.group().split(',')[0].upper())
                for i in range(len(addressProcessed)):
                    addressProcessed[i] = addressProcessed[i].strip()
                addressProcessed = ' '.join(addressProcessed)
            return addressProcessed
        
        data = {
                "industrial_zone" : [],
                "area_id": []
            }
        for id, address, area_id in id_addresses:
            industrialZone = extract_industrial_zone(address)
            if industrialZone == None:
                continue
            idZoneMap[id] = industrialZone
            data["industrial_zone"].append(industrialZone)
            data["area_id"].append(area_id)
        try:
            df = pd.DataFrame(data)
            for _, row in df.iterrows():
                zone = row['industrial_zone']
                area_id = row['area_id']
                cur.execute(
                    """
                    WITH e AS (
                        INSERT INTO industrial_parks (name, area_id)
                        VALUES
                            (%s, %s)
                        ON CONFLICT DO NOTHING
                        RETURNING id
                    )
                    SELECT * FROM e
                    UNION
                        SELECT id FROM industrial_parks WHERE name = %s
                    """, (zone, area_id, zone,)
                )
                zoneId = cur.fetchone()[0]
                industrialZoneMap[zone] = zoneId
            conn.commit()
            self.logger.info("Finish processing addresses and industrial zones")
            return industrialZoneMap, idZoneMap
        
        except Exception as e:
            conn.rollback()
            self.logger.error(f"Error extracting industrial zones from address: {str(e)}")
            raise
        finally:
            if conn:
                cur.close()
                conn.close()
            
    def address_to_zone(self) -> None:
        conn = psycopg2.connect(**self.db_params)
        cur = conn.cursor()
        try:
            industrialZoneMap, idZoneMap = self.process_industrial_parks()
            for business_id, zone in idZoneMap.items():
                cur.execute(
                    """
                    INSERT INTO industrial_zone_businesses (business_id, zone_id)
                    VALUES
                        (%s, %s)
                    ON CONFLICT DO NOTHING
                    """, (business_id, industrialZoneMap[zone])
                )
            conn.commit()
            self.logger.info("Finish filtering businesses")
        except Exception as e:
            conn.rollback()
            self.logger.error(str(e))
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
    industrialFilter = IndustrialZoneBusinessesFinder(db_params)
    try:
        industrialFilter.create_industrial_schema()
        industrialFilter.address_to_zone()
    except Exception as e:
        print(str(e))
        sys.exit(1)

if __name__ == "__main__":
    s1 = "Đăk Ơ"
    s2 = "Đak Ơ"
    s3 = "Hà Nội"
    print(f"Ratio(s1, s2) = {ratio(s1, s2)}, partial(s1, s2) = {partial_ratio(s1, s2)}"
          f"Ratio(s1, s3) = {ratio(s1, s3)}, partial(s1, s3) = {partial_ratio(s1, s3)}")
    
    test_arr = pd.DataFrame([(1, "Đak Ơ"), (2, "Bình Phước"), (3, "Đak Long"), (4,"Hà Nội")])
    a, b = max(test_arr.iloc(), key=lambda x: partial_ratio(x[1], s1))
    print(a, b)