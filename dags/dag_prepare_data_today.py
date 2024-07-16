
import os
import json
import shutil
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
# Airflow DAG definition
default_args =  {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# my function start here
def myfun():
    import pandas as pd
    from faker import Faker
    import random
    from datetime import datetime
    from datetime import datetime, timedelta
    # Initialize Faker
    fake = Faker()
    num_customers = 1000
    num_accounts = 2000
    num_transactions = 5000
    num_loans = 500
    num_cards = 1000
    num_branches = 100
    num_employees = 500
    num_atms = 200
    num_investments = 300
    num_customer_support = 300
    num_fixed_deposits = 400
    num_recurring_deposits = 400
    num_online_banking = 600
    num_bill_payments = 700
    num_insurance = 300
    num_credit_scores = 1000
    num_service_charges = 800
    num_cheques = 500
    num_savings_goals = 400
    num_mortgage_applications = 300
    # Define today's date
    today = datetime.today()
    date = datetime.today()
    # Function to generate data for today and save as CSV
    def generate_data(num_records, columns, generator_func, filename):
        data = [generator_func(today) for _ in range(num_records)]
        df = pd.DataFrame(data, columns=columns)
        df.to_csv(filename, index=False)
        return df
    path="/home/kali/Desktop/projects/git/bank_data_processing/dags/data_prepare/"
    # Generate Customers data for today
    df_customers=generate_data(
        num_customers,
        ['customer_id', 'name', 'address', 'phone', 'email', 'dob'],
        lambda date: [fake.uuid4(), fake.name(), fake.address(), fake.phone_number(), fake.email(), fake.date_of_birth(minimum_age=18, maximum_age=90)],
        f'{path}customers_today.csv'
    )

    # Generate Accounts data for today
    df_accounts=generate_data(
        num_accounts,
        ['account_id', 'customer_id', 'branch_id', 'account_type', 'balance', 'open_date'],
        lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), fake.uuid4(), random.choice(['savings', 'current']), round(random.uniform(1000, 100000), 2), fake.date_between(start_date=date, end_date=date+ timedelta(days=1))],
        f'{path}accounts_today.csv'
    )

    # Generate Transactions data for today
    generate_data(
        num_transactions,
        ['transaction_id', 'account_id', 'timestamp', 'amount', 'transaction_type', 'description'],
        lambda date: [fake.uuid4(), random.choice(df_accounts['account_id']), fake.date_time_between(start_date=date, end_date=date + timedelta(days=1)), round(random.uniform(-500, 5000), 2), random.choice(['debit', 'credit']), fake.sentence(nb_words=5)],
        f'{path}transactions_today.csv'
    )

    # Generate Loans data for today
    generate_data(
        num_loans,
        ['loan_id', 'customer_id', 'amount', 'interest_rate', 'start_date', 'end_date'],
        lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), round(random.uniform(10000, 500000), 2), round(random.uniform(1, 15), 2), fake.date_between(start_date=date, end_date=date + timedelta(days=1)), fake.date_between(start_date=date + timedelta(days=1), end_date=date + timedelta(days=365))],
        f'{path}loans_today.csv'
    )

    # Generate Cards data for today
    generate_data(
        num_cards,
        ['card_id', 'customer_id', 'card_number', 'card_type', 'expiration_date', 'security_code'],
        lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), fake.credit_card_number(), random.choice(['debit', 'credit']), fake.credit_card_expire(start='now', end='+4y'), fake.credit_card_security_code()],
        f'{path}cards_today.csv'
    )

    # Generate Branches data for today
    df_branches=generate_data(
        num_branches,
        ['branch_id', 'name', 'address', 'phone'],
        lambda date: [fake.uuid4(), fake.company(), fake.address(), fake.phone_number()],
        f'{path}branches_today.csv'
    )

    # Generate Employees data for today
    df_employees=generate_data(
        num_employees,
        ['employee_id', 'branch_id', 'name', 'position', 'salary', 'hire_date'],
        lambda date: [fake.uuid4(), random.choice(df_branches['branch_id']), fake.name(), random.choice(['teller', 'manager', 'customer service', 'security']), round(random.uniform(30000, 120000), 2), fake.date_between(start_date=date, end_date=date + timedelta(days=1))],
        f'{path}employees_today.csv'
    )

    # Generate ATMs data for today
    generate_data(
        num_atms,
        ['atm_id', 'branch_id', 'location', 'status', 'installation_date'],
        lambda date: [fake.uuid4(), random.choice(df_branches['branch_id']), fake.address(), random.choice(['active', 'maintenance', 'out of service']), fake.date_between(start_date=date, end_date=date + timedelta(days=1))],
        f'{path}atms_today.csv'
    )

    # Generate Investments data for today
    generate_data(
        num_investments,
        ['investment_id', 'customer_id', 'investment_type', 'amount', 'start_date', 'end_date'],
        lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), random.choice(['stocks', 'bonds', 'mutual funds', 'real estate']), round(random.uniform(5000, 200000), 2), fake.date_between(start_date=date, end_date=date + timedelta(days=1)), fake.date_between(start_date=date + timedelta(days=1), end_date=date + timedelta(days=365))],
        f'{path}investments_today.csv'
    )

    # Generate Customer Support data for today
    generate_data(
        num_customer_support,
        ['interaction_id', 'customer_id', 'employee_id', 'date', 'issue_type', 'resolution_status'],
        lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), random.choice(df_employees['employee_id']), fake.date_between(start_date=date, end_date=date + timedelta(days=1)), random.choice(['account issue', 'transaction dispute', 'loan inquiry', 'card issue', 'other']), random.choice(['resolved', 'pending', 'escalated'])],
        f'{path}customer_support_today.csv'
    )

    # Generate Fixed Deposits data for today
    generate_data(
        num_fixed_deposits,
        ['deposit_id', 'customer_id', 'amount', 'interest_rate', 'start_date', 'maturity_date'],
        lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), round(random.uniform(5000, 100000), 2), round(random.uniform(1, 10), 2), fake.date_between(start_date=date, end_date=date + timedelta(days=1)), fake.date_between(start_date=date + timedelta(days=1), end_date=date + timedelta(days=365))],
        f'{path}fixed_deposits_today.csv'
    )

    # Generate Recurring Deposits data for today
    generate_data(
        num_recurring_deposits,
        ['deposit_id', 'customer_id', 'monthly_amount', 'interest_rate', 'start_date', 'end_date'],
        lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), round(random.uniform(100, 2000), 2), round(random.uniform(1, 10), 2), fake.date_between(start_date=date, end_date=date + timedelta(days=1)), fake.date_between(start_date=date + timedelta(days=1), end_date=date + timedelta(days=365))],
        f'{path}recurring_deposits_today.csv'
    )

    # Generate Online Banking data for today
    generate_data(
        num_online_banking,
        ['login_id', 'customer_id', 'login_time', 'ip_address', 'device'],
        lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), fake.date_time_between(start_date=date, end_date=date + timedelta(days=1)), fake.ipv4(), fake.user_agent()],
        f'{path}online_banking_today.csv'
    )

    # Generate Bill Payments data for today
    generate_data(
        num_bill_payments,
        ['payment_id', 'customer_id', 'amount', 'bill_type', 'payment_date'],
        lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), round(random.uniform(50, 1000), 2), random.choice(['electricity', 'water', 'internet', 'phone']), fake.date_between(start_date=date, end_date=date + timedelta(days=1))],
        f'{path}bill_payments_today.csv'
    )

    # Generate Insurance data for today
    generate_data(
        num_insurance,
        ['policy_id', 'customer_id', 'policy_type', 'premium_amount', 'start_date', 'end_date'],
        lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), random.choice(['life', 'health', 'vehicle', 'home']), round(random.uniform(1000, 20000), 2), fake.date_between(start_date=date, end_date=date + timedelta(days=1)), fake.date_between(start_date=date + timedelta(days=1), end_date=date + timedelta(days=365))],
        f'{path}insurance_today.csv'
    )

    # Generate Credit Scores data for today
    generate_data(
        num_credit_scores,
        ['customer_id', 'credit_score', 'score_date'],
        lambda date: [random.choice(df_customers['customer_id']), random.randint(300, 850), fake.date_between(start_date=date, end_date=date + timedelta(days=1))],
        f'{path}credit_scores_today.csv'
    )

    # Generate Service Charges data for today
    generate_data(
        num_service_charges,
        ['charge_id', 'account_id', 'amount', 'charge_type', 'charge_date'],
        lambda date: [fake.uuid4(), random.choice(df_accounts['account_id']), round(random.uniform(5, 100), 2), random.choice(['maintenance fee', 'overdraft fee', 'ATM fee']), fake.date_between(start_date=date, end_date=date + timedelta(days=1))],
        f'{path}service_charges_today.csv'
    )

    # Generate Cheques data for today
    generate_data(
        num_cheques,
        ['cheque_id', 'account_id', 'amount', 'date_issued', 'date_cleared', 'status'],
        lambda date: [fake.uuid4(), random.choice(df_accounts['account_id']), round(random.uniform(100, 10000), 2), fake.date_between(start_date=date, end_date=date + timedelta(days=1)), fake.date_between(start_date=date, end_date=date + timedelta(days=7)), random.choice(['cleared', 'bounced', 'pending'])],
        f'{path}cheques_today.csv'
    )

    # Generate Savings Goals data for today
    generate_data(
        num_savings_goals,
        ['goal_id', 'customer_id', 'goal_name', 'target_amount', 'current_amount', 'start_date', 'end_date'],
        lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), fake.word(), round(random.uniform(500, 50000), 2), round(random.uniform(0, 50000), 2), fake.date_between(start_date=date, end_date=date + timedelta(days=1)), fake.date_between(start_date=date + timedelta(days=1), end_date=date + timedelta(days=365))],
        f'{path}savings_goals_today.csv'
    )

    # Generate Mortgage Applications data for today
    generate_data(
        num_mortgage_applications,
        ['application_id', 'customer_id', 'property_value', 'loan_amount', 'interest_rate', 'application_date', 'status'],
        lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), round(random.uniform(50000, 1000000), 2), round(random.uniform(50000, 1000000), 2), round(random.uniform(1, 10), 2), fake.date_between(start_date=date, end_date=date + timedelta(days=1)), random.choice(['approved', 'rejected', 'pending'])],
        f'{path}mortgage_applications_today.csv'
    )

    print("Real-time data generation for today is complete. CSV files have been saved.")

#  my func ends here




dag = DAG(
    'dag_prepare_data_today',
    default_args=default_args,
    description='Create random data files for all 20 tables in data prpare area',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1), # Adjust the start date as needed
    tags=['example'],
)

# Python operator to execute the load_json_files_to_mysql function
Prepare_data = PythonOperator(
    task_id='Prepare_data',
    python_callable=myfun,
    provide_context=True,  # This provides the task context (e.g., execution date)
    dag=dag,
)
# Define task dependencies
Prepare_data 
