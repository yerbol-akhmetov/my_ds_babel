import sqlite3
import csv

# creating class for writing csv string
class csvTextBuilder(object):
    def __init__(self):
        self.csv_string = []

    def write(self, row):
        self.csv_string.append(row)


# Function that converts data from SQL format to CSV format
def sql_to_csv(database, table_name):
    # loading the SQL database
    try:
        conn = sqlite3.connect(database)
    except Exception as e:
        print("Error while connecting: " + str(e))
    
    # creating CSV opject and writer
    csvfile = csvTextBuilder()
    writer = csv.writer(csvfile)

    # Retrieving column names and writing to CSV object
    db_colnames = conn.execute(f"SELECT name FROM pragma_table_info('{table_name}')")
    col_names = db_colnames.fetchall()
    col_names_clean = []
    for name in col_names:
        col_names_clean.append(name[0])
    writer.writerow(col_names_clean)

    # Retrieving content of table and writing it to CSV object
    db_content = conn.execute(F"SELECT * FROM {table_name}")
    writer.writerows(db_content)

    # Obtaining CSV string from CSV object
    csv_array = csvfile.csv_string
    csv_string = ''.join(csv_array)[:-1]

    # Closing the SQL query
    conn.close()

    return csv_string


db_filename = "all_fault_line.db"
table_name = "fault_lines"
csv_string = sql_to_csv(db_filename, table_name)
data = csv_string.split("\n")
#print(data)
