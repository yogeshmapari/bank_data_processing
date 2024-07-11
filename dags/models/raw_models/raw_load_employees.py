    
    

# def myfunc():

import pandas as pd
import datetime
#     # List of CSV files and their corresponding table names
 
    # Function to create tables if they do not exist
def create_tables(cursor):
        create_employees_table = """
        CREATE TABLE  IF NOT EXISTS employees (
    employee_id VARCHAR(255) PRIMARY KEY,
    branch_id VARCHAR(255),
    name VARCHAR(255),
    position VARCHAR(50),
    salary DECIMAL(10, 2),
    hire_date VARCHAR(20)
);
        """
        cursor.execute(create_employees_table)

def tuncate_table(cursor):
        tuncate_table = """
    truncate table employees
        ;
        """
        cursor.execute(tuncate_table)

def insert_table(cursor):
        csv_files = { '/home/kali/Desktop/projects/banking _project/data_prepare/employees_today.csv': 'employees' }
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

        for csv_file, table_name in csv_files.items():
            load_csv_to_mysql(csv_file, table_name)
            print(f"Loaded {csv_file} into {table_name} table")    
        print("All CSV files have been loaded into MySQL tables.")

    # Create tables if they do not exist
    # create_tables(cursor)
    # tuncate_table(cursor)
    # insert_table(cursor)

