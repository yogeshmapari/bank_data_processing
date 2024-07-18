    
    

# def myfunc():

import pandas as pd
import datetime
from airflow.hooks.mysql_hook import MySqlHook
#     # List of CSV files and their corresponding table names
import shutil
import os
from airflow.hooks.mysql_hook import MySqlHook

    # Function to create tables if they do not exist
def create_tables(cursor):
        
        create_employees_table = f"""
        
        CREATE TABLE  IF NOT EXISTS accounts (
        account_id VARCHAR(255) ,
        customer_id VARCHAR(255),
        branch_id VARCHAR(255),
        account_type VARCHAR(50),
        balance VARCHAR(255),
        open_date VARCHAR(255),
        status VARCHAR(50),
        ingetion_timestamp DATETIME
        );
        """
        cursor.execute(create_employees_table)

        # cursor.close()  # Close the cursor
def tuncate_table(cursor):
        tuncate_table = f"""
    
                truncate table accounts;
        """
        cursor.execute(tuncate_table)


def insert_table(cursor):
        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        conn = mysql_hook.get_conn()
        cursor=conn.cursor()

        csv_files = { '/home/kali/Desktop/projects/git/bank_data_processing/dags/data_prepare/accounts_today.csv': 'accounts' }
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

                sql = f"INSERT INTO {table_name} (`{cols}`) VALUES "
                fsql=sql+str(tuple(row))+';'
                print(fsql)
                cursor.execute(fsql)
                # results = cursor.fetchall()
            

        for csv_file, table_name in csv_files.items():
            import shutil
            import os
            if os.path.exists(csv_file):
                load_csv_to_mysql(csv_file, table_name)
                try:
                    cursor.execute("SELECT * FROM accounts")
                    results = cursor.fetchall()
    
                    for row in results:
                        print(row)
                finally:
                    cursor.close()  # Close the cursor
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

