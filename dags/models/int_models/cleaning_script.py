import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Function to remove leading and trailing spaces from all string columns
def strip_whitespace(df):
    str_cols = df.select_dtypes(include=['object']).columns
    df[str_cols] = df[str_cols].applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return df

# Function to convert empty strings to NaN for all columns
def empty_to_nan(df):
    df.replace('', np.nan, inplace=True)
    return df

# Function to drop columns with all NaN values
def drop_all_nan_columns(df):
    df.dropna(axis=1, how='all', inplace=True)
    return df

# Function to fill NaN values for all columns
def fill_na(df, value=None, method=None):
    if value is not None:
        df.fillna(value=value, inplace=True)
    elif method is not None:
        df.fillna(method=method, inplace=True)
    return df

# Function to convert all columns that can be to datetime
def convert_all_to_datetime(df):
    for col in df.columns:
        df[col] = pd.to_datetime(df[col], errors='ignore', infer_datetime_format=True)
    return df

# Function to convert all columns that can be to numeric (int or float)
def convert_all_to_numeric(df):
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='ignore')
    return df

# Function to remove duplicate rows
def remove_duplicates(df):
    df.drop_duplicates(inplace=True)
    return df

# Function to normalize column names
def normalize_column_names(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    return df

# Function to handle outliers by capping them to a specified quantile range
def handle_outliers(df, lower_quantile=0.01, upper_quantile=0.99):
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        lower_bound = df[col].quantile(lower_quantile)
        upper_bound = df[col].quantile(upper_quantile)
        df[col] = np.where(df[col] < lower_bound, lower_bound, df[col])
        df[col] = np.where(df[col] > upper_bound, upper_bound, df[col])
    return df

# Function to load data from MySQL table, transform it, and load it back to another MySQL table
def transform_and_load_data(connection_string, source_table, target_table):
    # Create SQLAlchemy engine
    engine = create_engine(connection_string)
    
    # Load data from source table
    df = pd.read_sql_table(source_table, engine)
    
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

    # Write transformed data to target table
    df.to_sql(target_table, engine, if_exists='replace', index=False)

# Example usage
if __name__ == "__main__":
    # Define your MySQL connection string
    connection_string = 'mysql+pymysql://username:password@hostname/dbname'
    
    # Source and target tables
    source_table = 'raw_data_table'
    target_table = 'cleaned_data_table'
    
    # Perform transformation and load data
    transform_and_load_data(connection_string, source_table, target_table)
