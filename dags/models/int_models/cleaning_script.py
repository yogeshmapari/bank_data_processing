from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import pandas as pd
import numpy as np
import mysql.connector

# Define functions for data transformations
def strip_whitespace(df):
    str_cols = df.select_dtypes(include=['object']).columns
    df[str_cols] = df[str_cols].applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return df

def empty_to_nan(df):
    df.replace('', np.nan, inplace=True)
    return df

def drop_all_nan_columns(df):
    df.dropna(axis=1, how='all', inplace=True)
    return df

def fill_na(df, value=None, method=None):
    if value is not None:
        df.fillna(value=value, inplace=True)
    elif method is not None:
        df.fillna(method=method, inplace=True)
    return df

def convert_all_to_datetime(df):
    for col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='ignore', infer_datetime_format=True)
    return df

def convert_all_to_numeric(df):
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='ignore')
    return df

def remove_duplicates(df):
    df.drop_duplicates(inplace=True)
    return df

def normalize_column_names(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    return df

def handle_outliers(df, lower_quantile=0.01, upper_quantile=0.99):
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        lower_bound = df[col].quantile(lower_quantile)
        upper_bound = df[col].quantile(upper_quantile)
        df[col] = np.where(df[col] < lower_bound, lower_bound, df[col])
        df[col] = np.where(df[col] > upper_bound, upper_bound, df[col])
    return df

def transform_and_load_data(mysql_conn_id, source_table, target_table):
    # Create MySQL hook
    mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    
    # Get MySQL connection
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    
    # Load data from source table into DataFrame
    df = pd.read_sql(f'SELECT * FROM {source_table}', con=connection)
    
    # Apply transformations
    df = strip_whitespace(df)
    df = empty_to_nan(df)
    df = drop_all_nan_columns(df)
    df = fill_na(df, value=0)  # Fill all NaNs with 0
    df = convert_all_to_datetime(df)
    df = convert_all_to_numeric(df)
    df = remove_duplicates(df)
    df = normalize_column_names(df)
    df = handle_outliers(df)
    

        # Add timestamp as a new column
    df['int_ingetion_timestamp'] = datetime.now()
    print(df.columns,df.dtypes)
    # Create table if it doesn't exist
    cursor.execute(f"""
                   drop TABLE IF  EXISTS {target_table};
    CREATE TABLE IF NOT EXISTS {target_table} (
        {', '.join([f'{col} {dtype}' for col, dtype in zip(df.columns, df.dtypes.replace({'object': 'VARCHAR(255)', 'int64': 'INT', 'float64': 'FLOAT', 'datetime64[us]': 'DATETIME','datetime64': 'DATETIME'}))])}
    )
    """)

    
    # Insert data into target table
    for _, row in df.iterrows():
        cursor.execute(f"""
        INSERT INTO {target_table} ({', '.join(df.columns)})
        VALUES ({', '.join(['%s'] * len(df.columns))})
        """, tuple(row))
    
    # Commit changes
    connection.commit()
    cursor.close()
    connection.close()

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the list of source and target table pairs
table_pairs = [
('accounts','int_accounts'),
('atms','int_atms'),
('bill_payments','int_bill_payments'),
('branches','int_branches'),
('cards','int_cards'),
('cheques','int_cheques'),
('credit_scores','int_credit_scores'),
('customer_support','int_customer_support'),
('customers','int_customers'),
('employees','int_employees'),
('fixed_deposits','int_fixed_deposits'),
('insurance','int_insurance'),
('investments','int_investments'),
('loans','int_loans'),
('mortgage_applications','int_mortgage_applications'),
('online_banking','int_online_banking'),
('recurring_deposits','int_recurring_deposits'),
('savings_goals','int_savings_goals'),
('service_charges','int_service_charges'),
('transactions','int_transactions')
    # Add more table pairs as needed
]

# Define the DAG
with DAG(
    'mysql_data_transformation',
    default_args=default_args,
    description='A DAG to transform data from MySQL tables and load into other tables',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    # # Dynamically create tasks for each table pair
    # for source_table, target_table in table_pairs:
    #     task = PythonOperator(
    #         task_id=f'transform_and_load_{source_table}_to_{target_table}',
    #         python_callable=transform_and_load_data,
    #         op_kwargs={
    #             'mysql_conn_id': 'mysql_default',  # Replace with your MySQL connection ID in Airflow
    #             'source_table': source_table,
    #             'target_table': target_table
    #         },
    #     )
    
    previous_task = None  # To keep track of the last task created
    
    # Dynamically create tasks for each table pair
    for source_table, target_table in table_pairs:
        task = PythonOperator(
            task_id=f'transform_and_load_{source_table}_to_{target_table}',
            python_callable=transform_and_load_data,
            op_kwargs={
                'mysql_conn_id': 'mysql_default',  # Replace with your MySQL connection ID in Airflow
                'source_table': source_table,
                'target_table': target_table
            },
        )
        
        # Set linear dependencies
        if previous_task:
            previous_task >> task  # Set the previous task to run before the current task
        
        previous_task = task  # Update previous_task to the current task