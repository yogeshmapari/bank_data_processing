import pandas as pd
from sqlalchemy import create_engine
import configparser
import MySQLdb
import datetime
# Database connection details
config = configparser.ConfigParser()
config.read('config.ini')

        # Get the MySQL database configuration

user = 'root'
password = 'root'
host = 'localhost'
database = 'bank_raw_layer'
        
conn = MySQLdb.connect(
    host=host,
    user=user,
    passwd=password,
    db=database
)

# Create a cursor object
cursor = conn.cursor()
# engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')

# List of CSV files and their corresponding table names
csv_files = {
    'data_prepare/accounts_today.csv': 'accounts'
    # Add the rest of your CSV files here
}

# Function to create tables if they do not exist
def create_tables():
    create_employees_table = """
    CREATE TABLE  IF NOT EXISTS accounts (
    account_id VARCHAR(255) ,
    customer_id VARCHAR(255),
    branch_id VARCHAR(255),
    account_type VARCHAR(50),
    balance VARCHAR(255),
    open_date VARCHAR(255),
    status VARCHAR(50),
    ingetion_timestamp VARCHAR(255)
    );
    """
    cursor.execute(create_employees_table)

def tuncate_table():
    tuncate_table = """
truncate table accounts
    ;
    """
    cursor.execute(tuncate_table)

def insert_table():
    for csv_file, table_name in csv_files.items():
        load_csv_to_mysql(csv_file, table_name)
        print(f"Loaded {csv_file} into {table_name} table")


# Function to load CSV into MySQL table
def load_csv_to_mysql(csv_file, table_name):
    df = pd.read_csv(csv_file)
    current_timestamp = datetime.datetime.now()

# Add timestamp as a new column
    df['ingetion_timestamp'] = current_timestamp
    # Prepare the insert query string
    cols = "`,`".join([str(i) for i in df.columns.tolist()])
    print(cols)

    for i, row in df.iterrows():
        sql = f"INSERT INTO `{table_name}` (`{cols}`) VALUES ({'%s, ' * (len(row) - 1)}%s)"
        print(tuple(row))
        cursor.execute(sql, tuple(row))
    conn.commit()

# Create tables if they do not exist
create_tables()
tuncate_table()
insert_table()
# Load each CSV into the corresponding MySQL table


# Close the cursor and connection
cursor.close()
conn.close()

print("All CSV files have been loaded into MySQL tables.")