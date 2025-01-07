def main():
    while True:
        try:
            option = input("1. Read excel file\n"
                           "2. Query database\n"
                           "0. Exit\n"
                           "Option: ")
            if not 0 <= int(option) <= 2:
                print("Invalid option")
                continue
            
        except KeyboardInterrupt:
            return
        except:
            print("Invalid option")
            continue

if __name__ == "__main__":
    main()