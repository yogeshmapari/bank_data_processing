    
    

# def myfunc():

import pandas as pd
import datetime
from airflow.hooks.mysql_hook import MySqlHook
#     # List of CSV files and their corresponding table names
 
    # Function to create tables if they do not exist
def create_tables(cursor):
        create_employees_table = """
        Drop TABLE IF EXISTS cheques;
        CREATE TABLE  IF NOT EXISTS cheques (
    cheque_id VARCHAR(255) PRIMARY KEY,
    account_id VARCHAR(255),
    amount DECIMAL(15, 2),
    date_issued VARCHAR(20),
    date_cleared VARCHAR(20),
    status VARCHAR(50),
        ingetion_timestamp DATETIME
);
        """
        cursor.execute(create_employees_table)

def tuncate_table(cursor):
        tuncate_table = """
    truncate table cheques
        ;
        """
        cursor.execute(tuncate_table)

def insert_table(cursor):
        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        conn = mysql_hook.get_conn()
        cursor=conn.cursor()
        csv_files = { '/home/kali/Desktop/projects/git/bank_data_processing/dags/data_prepare/cheques_today.csv': 'cheques' }
        def load_csv_to_mysql(csv_file, table_name):
            df = pd.read_csv(csv_file)
            current_timestamp = datetime.datetime.now()

        # Add timestamp as a new column
            df['ingetion_timestamp'] = current_timestamp
            # Prepare the insert query string
            cols = "`,`".join([str(i) for i in df.columns.tolist()])
            print(cols)

            for i, row in df.iterrows():
                row = [str(item) for item in row]
                sql = f"INSERT INTO `{table_name}` (`{cols}`) VALUES ({'%s, ' * (len(row) - 1)}%s)"
                print(tuple(row))
                cursor.execute(sql, tuple(row))

        for csv_file, table_name in csv_files.items():
            import shutil
            import os
            if os.path.exists(csv_file):
                load_csv_to_mysql(csv_file, table_name)
                print(f"Loaded {csv_file} into {table_name} table")
            
            # Move the file to the archive folder
                archive_folder = '/home/kali/Desktop/projects/git/bank_data_processing/archive/'
                if not os.path.exists(archive_folder):
                    os.makedirs(archive_folder)
                shutil.move(csv_file, os.path.join(archive_folder, os.path.basename(f"{csv_file}_{datetime.datetime.now()}")))
                print(f"Moved {csv_file} to archive folder")
            else:
                print(f"File not found: {csv_file}")
 
        print("All CSV files have been loaded into MySQL tables.")
        conn.commit()
    # Create tables if they do not exist
    # create_tables(cursor)
    # tuncate_table(cursor)
    # insert_table(cursor)

