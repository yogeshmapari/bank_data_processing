    
    

# def myfunc():

import pandas as pd
import datetime
#     # List of CSV files and their corresponding table names
 
    # Function to create tables if they do not exist
def create_tables(cursor):
        create_employees_table = """
        Drop TABLE IF EXISTS transactions;
        CREATE TABLE  IF NOT EXISTS transactions (
    transaction_id VARCHAR(255) ,
    account_id VARCHAR(255),
    timestamp VARCHAR(255),
    amount DECIMAL(15, 2),
    transaction_type VARCHAR(50),
    description TEXT,
        ingetion_timestamp VARCHAR(255)
);
        """
        cursor.execute(create_employees_table)

def tuncate_table(cursor):
        tuncate_table = """
    truncate table transactions
        ;
        """
        cursor.execute(tuncate_table)

def insert_table(cursor):
        csv_files = { '/home/kali/Desktop/projects/git/bank_data_processing/data_prepare/transactions_today.csv': 'transactions' }
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

    # Create tables if they do not exist
    # create_tables(cursor)
    # tuncate_table(cursor)
    # insert_table(cursor)

