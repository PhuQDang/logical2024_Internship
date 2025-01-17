import pandas as pd
import psycopg2
import logging
import sys
import re
from thefuzz import fuzz

class IndustrialParkClassifier:
    def __init__(self, db_params):
        self.db_params = db_params
        self.__setup_logging()
        self.__get_industrial_parks()

    def __setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('industrial_classifier.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def __get_industrial_parks(self):
        try:
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            cur.execute("""
                SELECT industrial_parks.id as park_id,
                       industrial_parks.name as park_name
                FROM industrial_parks
            """)
            df = pd.DataFrame(cur.fetchall())
            df.columns = ['park_id', 'park_name']
            for i, row in df.iterrows():
                df.loc[i, ['park_name']] = self.roman_to_int(row['park_name'])
            self.parks = df
            self.logger.info("Finish getting industrial parks")
        except Exception as e:
            conn.rollback()
            self.logger.error(str(e))
            raise
        finally:
            if conn:
                cur.close()
                conn.close()

    def extract_zone(self, address: str) -> str|None:
        unprocessedZone = re.search(r"(?<=KCN )[\D\d]+?,|(?<=Khu công nghiệp )[\D\d]+?,|(?<=Khu Công Nghiệp )[\D\d]+?,|(?<=khu công nghiệp )[\D\d]+?,", address)
        if unprocessedZone != None:
            unprocessedZone = re.findall(r"[^–()-]+", unprocessedZone.group().split(',')[0].upper())
            unprocessedZone = ' '.join(list(map(lambda x: x.strip(), unprocessedZone)))
        return unprocessedZone

    def int_to_roman(self, text: str) -> str:
        number_in_text = re.search(r"\d+", text)
        if number_in_text == None:
            return text
        number_in_text = int(number_in_text.group())
        roman_numerals = ['I', 'IV', 'V', 'IX', 'X']
        numerals = [1, 4, 5, 9, 10]
        roman_conversion = ""
        i = len(roman_numerals)-1
        while number_in_text:
            t = number_in_text//numerals[i]
            number_in_text = number_in_text%numerals[i]
            for _ in range(t):
                roman_conversion += roman_numerals[i]
            i -= 1
        return re.search(r"[\D]+(?=\d)", text).group() + roman_conversion
    
    def roman_to_int(self, text: str) -> str:
        roman_ = re.search(r" [IVX]+$", text)
        if roman_ == None:
            return text
        roman_str = roman_.group().strip()
        num = 0
        translation = {"X": 10, "V": 5, "I": 1}
        roman_str = roman_str.replace("IV", "IIII").replace("IX", "VIIII")
        for c in roman_str:
            num += translation[c]
        return re.search(r"[\D]+ (?=[IVX]+$)", text).group() + str(num)

    def classify_(self, address) -> int|None:
        """
        Classify the industrial park where the business belongs based on string matching with
        industrial parks name already available in the database
        *This method does not account for new industrial park being discovered during the process
        of matching
        """
        unprocessed_address = self.extract_zone(address)
        if unprocessed_address == None:
            return None
        processed_address = self.roman_to_int(unprocessed_address)
        _id, _ = max([(i, park) for i, park in self.parks.iloc()], key=lambda x: fuzz.partial_ratio(x[1], processed_address))
        return int(_id)

def main():
    try:
        db_params = {
        'host': 'localhost',
        'database': 'businessesdb',
        'user': 'postgres',
        'password': '1234',
        'port': '5432'
        }
        classifier = IndustrialParkClassifier(db_params=db_params)
        test1 = classifier.classify_("Khu công nghiệp Tân Khai, Thị trấn Tân Khai, Huyện Hớn Quản, Tỉnh Bình Phước, Việt Nam")
        print(test1)
    except Exception as e:
        print(e)
        sys.exit(1)

if __name__ == "__main__":
    main()
