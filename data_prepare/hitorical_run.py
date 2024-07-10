import pandas as pd
from faker import Faker
import random
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
# Define the start date and end date for historical data
start_date = datetime(2023, 1, 1)
end_date = datetime.today()

# Function to generate data for a specific date range and save as CSV
def generate_data(num_records, columns, generator_func, filename, start_date, end_date):
    data = []
    current_date = start_date
    while current_date <= end_date:
        for _ in range(num_records):
            row = generator_func(current_date)
            data.append(row)
            print(current_date)
        current_date += timedelta(days=1)
    df = pd.DataFrame(data, columns=columns)
    df.to_csv(filename, index=False)

# Generate Customers data
generate_data(
    num_customers,
    ['customer_id', 'name', 'address', 'phone', 'email', 'dob'],
    lambda date: [fake.uuid4(), fake.name(), fake.address(), fake.phone_number(), fake.email(), fake.date_of_birth(minimum_age=18, maximum_age=90)],
    'customers_historical.csv',
    start_date,
    end_date
)

# Generate Accounts data
generate_data(
    num_accounts,
    ['account_id', 'customer_id', 'branch_id', 'account_type', 'balance', 'open_date'],
    lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), fake.uuid4(), random.choice(['savings', 'current']), round(random.uniform(1000, 100000), 2), fake.date_between_dates(start_date=date, end_date=date)],
    'accounts_historical.csv',
    start_date,
    end_date
)

# Generate Transactions data
generate_data(
    num_transactions,
    ['transaction_id', 'account_id', 'timestamp', 'amount', 'transaction_type', 'description'],
    lambda date: [fake.uuid4(), random.choice(df_accounts['account_id']), fake.date_time_between(start_date=date, end_date=date + timedelta(days=1)), round(random.uniform(-500, 5000), 2), random.choice(['debit', 'credit']), fake.sentence(nb_words=5)],
    'transactions_historical.csv',
    start_date,
    end_date
)

# Generate Loans data
generate_data(
    num_loans,
    ['loan_id', 'customer_id', 'amount', 'interest_rate', 'start_date', 'end_date'],
    lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), round(random.uniform(10000, 500000), 2), round(random.uniform(1, 15), 2), fake.date_between_dates(start_date=date, end_date=date + timedelta(days=1)), fake.date_between_dates(start_date=date + timedelta(days=1), end_date=date + timedelta(days=365))],
    'loans_historical.csv',
    start_date,
    end_date
)

# Generate Cards data
generate_data(
    num_cards,
    ['card_id', 'customer_id', 'card_number', 'card_type', 'expiration_date', 'security_code'],
    lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), fake.credit_card_number(), random.choice(['debit', 'credit']), fake.credit_card_expire(start='now', end='+4y'), fake.credit_card_security_code()],
    'cards_historical.csv',
    start_date,
    end_date
)

# Generate Branches data
generate_data(
    num_branches,
    ['branch_id', 'name', 'address', 'phone'],
    lambda date: [fake.uuid4(), fake.company(), fake.address(), fake.phone_number()],
    'branches_historical.csv',
    start_date,
    end_date
)

# Generate Employees data
generate_data(
    num_employees,
    ['employee_id', 'branch_id', 'name', 'position', 'salary', 'hire_date'],
    lambda date: [fake.uuid4(), random.choice(df_branches['branch_id']), fake.name(), random.choice(['teller', 'manager', 'customer service', 'security']), round(random.uniform(30000, 120000), 2), fake.date_between_dates(start_date=date, end_date=date + timedelta(days=1))],
    'employees_historical.csv',
    start_date,
    end_date
)

# Generate ATMs data
generate_data(
    num_atms,
    ['atm_id', 'branch_id', 'location', 'status', 'installation_date'],
    lambda date: [fake.uuid4(), random.choice(df_branches['branch_id']), fake.address(), random.choice(['active', 'maintenance', 'out of service']), fake.date_between_dates(start_date=date, end_date=date + timedelta(days=1))],
    'atms_historical.csv',
    start_date,
    end_date
)

# Generate Investments data
generate_data(
    num_investments,
    ['investment_id', 'customer_id', 'investment_type', 'amount', 'start_date', 'end_date'],
    lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), random.choice(['stocks', 'bonds', 'mutual funds', 'real estate']), round(random.uniform(5000, 200000), 2), fake.date_between_dates(start_date=date, end_date=date + timedelta(days=1)), fake.date_between_dates(start_date=date + timedelta(days=1), end_date=date + timedelta(days=365))],
    'investments_historical.csv',
    start_date,
    end_date
)

# Generate Customer Support data
generate_data(
    num_customer_support,
    ['interaction_id', 'customer_id', 'employee_id', 'date', 'issue_type', 'resolution_status'],
    lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), random.choice(df_employees['employee_id']), fake.date_between_dates(start_date=date, end_date=date + timedelta(days=1)), random.choice(['account issue', 'transaction dispute', 'loan inquiry', 'card issue', 'other']), random.choice(['resolved', 'pending', 'escalated'])],
    'customer_support_historical.csv',
    start_date,
    end_date
)

# Generate Fixed Deposits data
generate_data(
    num_fixed_deposits,
    ['deposit_id', 'customer_id', 'amount', 'interest_rate', 'start_date', 'maturity_date'],
    lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), round(random.uniform(5000, 100000), 2), round(random.uniform(1, 10), 2), fake.date_between_dates(start_date=date, end_date=date + timedelta(days=1)), fake.date_between_dates(start_date=date + timedelta(days=1), end_date=date + timedelta(days=365))],
    'fixed_deposits_historical.csv',
    start_date,
    end_date
)

# Generate Recurring Deposits data
generate_data(
    num_recurring_deposits,
    ['deposit_id', 'customer_id', 'monthly_amount', 'interest_rate', 'start_date', 'end_date'],
    lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), round(random.uniform(100, 2000), 2), round(random.uniform(1, 10), 2), fake.date_between_dates(start_date=date, end_date=date + timedelta(days=1)), fake.date_between_dates(start_date=date + timedelta(days=1), end_date=date + timedelta(days=365))],
    'recurring_deposits_historical.csv',
    start_date,
    end_date
)

# Generate Online Banking data
generate_data(
    num_online_banking,
    ['login_id', 'customer_id', 'login_time', 'ip_address', 'device'],
    lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), fake.date_time_between(start_date=date, end_date=date + timedelta(days=1)), fake.ipv4(), fake.user_agent()],
    'online_banking_historical.csv',
    start_date,
    end_date
)

# Generate Bill Payments data
generate_data(
    num_bill_payments,
    ['payment_id', 'customer_id', 'amount', 'bill_type', 'payment_date'],
    lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), round(random.uniform(50, 1000), 2), random.choice(['electricity', 'water', 'internet', 'phone']), fake.date_between_dates(start_date=date, end_date=date + timedelta(days=1))],
    'bill_payments_historical.csv',
    start_date,
    end_date
)

# Generate Insurance data
generate_data(
    num_insurance,
    ['policy_id', 'customer_id', 'policy_type', 'premium_amount', 'start_date', 'end_date'],
    lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), random.choice(['life', 'health', 'vehicle', 'home']), round(random.uniform(1000, 20000), 2), fake.date_between_dates(start_date=date, end_date=date + timedelta(days=1)), fake.date_between_dates(start_date=date + timedelta(days=1), end_date=date + timedelta(days=365))],
    'insurance_historical.csv',
    start_date,
    end_date
)

# Generate Credit Scores data
generate_data(
    num_credit_scores,
    ['customer_id', 'credit_score', 'score_date'],
    lambda date: [random.choice(df_customers['customer_id']), random.randint(300, 850), fake.date_between_dates(start_date=date, end_date=date + timedelta(days=1))],
    'credit_scores_historical.csv',
    start_date,
    end_date
)

# Generate Service Charges data
generate_data(
    num_service_charges,
    ['charge_id', 'account_id', 'amount', 'charge_type', 'charge_date'],
    lambda date: [fake.uuid4(), random.choice(df_accounts['account_id']), round(random.uniform(5, 100), 2), random.choice(['maintenance fee', 'overdraft fee', 'ATM fee']), fake.date_between_dates(start_date=date, end_date=date + timedelta(days=1))],
    'service_charges_historical.csv',
    start_date,
    end_date
)

# Generate Cheques data
generate_data(
    num_cheques,
    ['cheque_id', 'account_id', 'amount', 'date_issued', 'date_cleared', 'status'],
    lambda date: [fake.uuid4(), random.choice(df_accounts['account_id']), round(random.uniform(100, 10000), 2), fake.date_between_dates(start_date=date, end_date=date + timedelta(days=1)), fake.date_between_dates(start_date=date, end_date=date + timedelta(days=7)), random.choice(['cleared', 'bounced', 'pending'])],
    'cheques_historical.csv',
    start_date,
    end_date
)

# Generate Savings Goals data
generate_data(
    num_savings_goals,
    ['goal_id', 'customer_id', 'goal_name', 'target_amount', 'current_amount', 'start_date', 'end_date'],
    lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), fake.word(), round(random.uniform(500, 50000), 2), round(random.uniform(0, 50000), 2), fake.date_between_dates(start_date=date, end_date=date + timedelta(days=1)), fake.date_between_dates(start_date=date + timedelta(days=1), end_date=date + timedelta(days=365))],
    'savings_goals_historical.csv',
    start_date,
    end_date
)

# Generate Mortgage Applications data
generate_data(
    num_mortgage_applications,
    ['application_id', 'customer_id', 'property_value', 'loan_amount', 'interest_rate', 'application_date', 'status'],
    lambda date: [fake.uuid4(), random.choice(df_customers['customer_id']), round(random.uniform(50000, 1000000), 2), round(random.uniform(50000, 1000000), 2), round(random.uniform(1, 10), 2), fake.date_between_dates(start_date=date, end_date=date + timedelta(days=1)), random.choice(['approved', 'rejected', 'pending'])],
    'mortgage_applications_historical.csv',
    start_date,
    end_date
)

print("Historical data generation complete. CSV files have been saved.")
