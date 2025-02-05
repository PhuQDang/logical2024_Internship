from general_database import *
from query_functions import *

def general_setup(fname):
    db_params = {
        'host': 'localhost',
        'database': 'businessesdb',
        'user': 'postgres',
        'password': '1234',
        'port': '5432'
    }
    excel_file = fname
    
    # Create importer and run import
    importer = VNBusinessImporter(db_params, excel_file)
    
    try:
        importer.import_data()
        print("Data import completed successfully!")
    except Exception as e:
        print(f"Error: {str(e)}")
        return

def main():
    db_params = {
        'host': 'localhost',
        'database': 'businessesdb',
        'user': 'postgres',
        'password': '1234',
        'port': '5432'
    }
    while True:
        try:
            option = input("1. Read excel file\n"
                           "2. Query database\n"
                           "0. Exit\n"
                           "Option: ")
            if not 0 <= int(option) <= 2:
                print("Invalid option")
                continue
            elif int(option) == 0:
                sys.exit(0)
            elif int(option) == 1:
                fpath = input()
            elif int(option) == 2:
                queryRespond = QueryPrompter(db_parms=db_params)
                queryRespond.query_results()
        except KeyboardInterrupt:
            sys.exit(0)
        except ValueError:
            print("Invalid option")
            continue
        except Exception as e:
            print(f"Error: {str(e)}")
            sys.exit(1)

if __name__ == "__main__":
    fname = 'dsdn_1997_2024_processed.xlsx'
    start = time.time()
    general_setup(fname)
    end = time.time()
    print(f"Time taken to import all data = {end-start}")