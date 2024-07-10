import pandas as pd
from sqlalchemy import create_engine
import configparser
import MySQLdb
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
# Function to create tables if they do not exist
def create_tables():
    create_customers_table = """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id VARCHAR(255) ,
        name VARCHAR(255),
        address VARCHAR(255),
        phone VARCHAR(255),
        email VARCHAR(255),
        dob VARCHAR(255)
    );
    """
    
    create_branches_table = """
    CREATE TABLE IF NOT EXISTS branches (
        branch_id VARCHAR(255) ,
        name VARCHAR(255),
        address VARCHAR(255),
        phone VARCHAR(255)
    );
    """
    
    create_accounts_table = """
    CREATE TABLE IF NOT EXISTS accounts (
        account_id VARCHAR(255) ,
        customer_id VARCHAR(255),
        branch_id VARCHAR(255),
        account_type VARCHAR(50),
        balance VARCHAR(255),
        open_date VARCHAR(255)
    );
    """
    
    create_transactions_table = """
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id VARCHAR(255) ,
        account_id VARCHAR(255),
        timestamp VARCHAR(255),
        amount VARCHAR(255),
        transaction_type VARCHAR(50),
        description TEXT
    );
    """
    
    create_loans_table = """
    CREATE TABLE IF NOT EXISTS loans (
        loan_id VARCHAR(255) ,
        customer_id VARCHAR(255),
        amount VARCHAR(255),
        interest_rate VARCHAR(255),
        start_date VARCHAR(255),
        end_date VARCHAR(255)
    );
    """
    
    create_cards_table = """
    CREATE TABLE IF NOT EXISTS cards (
        card_id VARCHAR(255) ,
        customer_id VARCHAR(255),
        card_number VARCHAR(255),
        card_type VARCHAR(50),
        expiration_date VARCHAR(255),
        security_code VARCHAR(10)
        
    );
    """
    
    create_employees_table = """
    CREATE TABLE IF NOT EXISTS employees (
        employee_id VARCHAR(255) ,
        branch_id VARCHAR(255),
        name VARCHAR(255),
        position VARCHAR(50),
        salary VARCHAR(255),
        hire_date VARCHAR(255)
    );
    """
    
    cursor.execute(create_customers_table)
    cursor.execute(create_branches_table)
    cursor.execute(create_accounts_table)
    cursor.execute(create_transactions_table)
    cursor.execute(create_loans_table)
    cursor.execute(create_cards_table)
    cursor.execute(create_employees_table)
# List of CSV files and their corresponding table names
csv_files = {
    'data_prepare/customers_today.csv': 'customers',
    'data_prepare/branches_today.csv': 'branches',
    'data_prepare/accounts_today.csv': 'accounts',
    'data_prepare/transactions_today.csv': 'transactions',
    'data_prepare/loans_today.csv': 'loans',
    'data_prepare/cards_today.csv': 'cards',
    'data_prepare/employees_today.csv': 'employees'
    # Add the rest of your CSV files here
}

# Function to load CSV into MySQL table
def load_csv_to_mysql(csv_file, table_name):
    df = pd.read_csv(csv_file)
    # Prepare the insert query string
    cols = "`,`".join([str(i) for i in df.columns.tolist()])

    for i, row in df.iterrows():
        sql = f"INSERT INTO `{table_name}` (`{cols}`) VALUES ({'%s, ' * (len(row) - 1)}%s)"
        cursor.execute(sql, tuple(row))

    conn.commit()

# Create tables if they do not exist
create_tables()
# Load each CSV into the corresponding MySQL table
for csv_file, table_name in csv_files.items():
    load_csv_to_mysql(csv_file, table_name)
    print(f"Loaded {csv_file} into {table_name} table")

# Close the cursor and connection
cursor.close()
conn.close()

print("All CSV files have been loaded into MySQL tables.")