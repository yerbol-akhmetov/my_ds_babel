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
    writer = csv.writer(csvfile, lineterminator='\n')

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


# Function that converts data from CSV formatted string to SQL formatted database
def csv_to_sql(csv_content, database, table_name):
    rows = []
    csvreader = csv.reader(csv_content)
    
    # create a file of SQL database and cursor
    conn = sqlite3.connect(database)
    cur = conn.cursor()
    
    # retrieve fieldnames from CSV string
    fieldnames = next(csvreader)
    
    # create table if not exist with column names
    cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ([{fieldnames[0]}] varchar(255), [{fieldnames[1]}] varchar(255), [{fieldnames[2]}] varchar(255), [{fieldnames[3]}] varchar(255), [{fieldnames[4]}] varchar(255), [{fieldnames[5]}] varchar(255))")
    
    # loading all data to database
    insert_query = f"INSERT INTO {table_name} VALUES "
    for row in csvreader:
        while len(row) < 6:
            row.append('NULL')
        line = f"{row}".replace('[','(').replace(']',')')
        insert_query += line + ', '
    cur.execute(insert_query.strip(', '))

    # commiting the changes and closing objects
    conn.commit()
    cur.close()
    conn.close()
    

db_filename = "all_fault_line.db"
table_name = "fault_lines"
csv_string = sql_to_csv(db_filename, table_name)
data = csv_string.split("\n")
#print(data)
