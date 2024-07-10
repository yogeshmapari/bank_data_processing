import pandas as pd
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# Define the number of records for each table
num_customers = 1000
num_accounts = 1500
num_transactions = 10000
num_loans = 500
num_cards = 1000
num_branches = 100
num_employees = 300
num_atms = 200
num_investments = 600
num_customer_support = 1000

# Generate Customers data
customers = []
for _ in range(num_customers):
    customer_id = fake.uuid4()
    name = fake.name()
    address = fake.address()
    phone = fake.phone_number()
    email = fake.email()
    dob = fake.date_of_birth(minimum_age=18, maximum_age=90)
    customers.append([customer_id, name, address, phone, email, dob])

df_customers = pd.DataFrame(customers, columns=['customer_id', 'name', 'address', 'phone', 'email', 'dob'])
df_customers.to_csv('customers.csv', index=False)

# Generate Accounts data
accounts = []
for _ in range(num_accounts):
    account_id = fake.uuid4()
    customer_id = random.choice(df_customers['customer_id'])
    branch_id = fake.uuid4()
    account_type = random.choice(['savings', 'current'])
    balance = round(random.uniform(1000, 100000), 2)
    open_date = fake.date_this_decade()
    accounts.append([account_id, customer_id, branch_id, account_type, balance, open_date])

df_accounts = pd.DataFrame(accounts, columns=['account_id', 'customer_id', 'branch_id', 'account_type', 'balance', 'open_date'])
df_accounts.to_csv('accounts.csv', index=False)

# Generate Transactions data
transactions = []
for _ in range(num_transactions):
    transaction_id = fake.uuid4()
    account_id = random.choice(df_accounts['account_id'])
    timestamp = fake.date_time_this_year()
    amount = round(random.uniform(-500, 5000), 2)
    transaction_type = random.choice(['debit', 'credit'])
    description = fake.sentence(nb_words=5)
    transactions.append([transaction_id, account_id, timestamp, amount, transaction_type, description])

df_transactions = pd.DataFrame(transactions, columns=['transaction_id', 'account_id', 'timestamp', 'amount', 'transaction_type', 'description'])
df_transactions.to_csv('transactions.csv', index=False)

# Generate Loans data
loans = []
for _ in range(num_loans):
    loan_id = fake.uuid4()
    customer_id = random.choice(df_customers['customer_id'])
    amount = round(random.uniform(10000, 500000), 2)
    interest_rate = round(random.uniform(1, 15), 2)
    start_date = fake.date_this_decade()
    end_date = fake.date_this_century()
    loans.append([loan_id, customer_id, amount, interest_rate, start_date, end_date])

df_loans = pd.DataFrame(loans, columns=['loan_id', 'customer_id', 'amount', 'interest_rate', 'start_date', 'end_date'])
df_loans.to_csv('loans.csv', index=False)

# Generate Cards data
cards = []
for _ in range(num_cards):
    card_id = fake.uuid4()
    customer_id = random.choice(df_customers['customer_id'])
    card_number = fake.credit_card_number()
    card_type = random.choice(['debit', 'credit'])
    expiration_date = fake.credit_card_expire()
    security_code = fake.credit_card_security_code()
    cards.append([card_id, customer_id, card_number, card_type, expiration_date, security_code])

df_cards = pd.DataFrame(cards, columns=['card_id', 'customer_id', 'card_number', 'card_type', 'expiration_date', 'security_code'])
df_cards.to_csv('cards.csv', index=False)

# Generate Branches data
branches = []
for _ in range(num_branches):
    branch_id = fake.uuid4()
    name = fake.company()
    address = fake.address()
    phone = fake.phone_number()
    branches.append([branch_id, name, address, phone])

df_branches = pd.DataFrame(branches, columns=['branch_id', 'name', 'address', 'phone'])
df_branches.to_csv('branches.csv', index=False)

# Generate Employees data
employees = []
for _ in range(num_employees):
    employee_id = fake.uuid4()
    branch_id = random.choice(df_branches['branch_id'])
    name = fake.name()
    position = random.choice(['teller', 'manager', 'customer service', 'security'])
    salary = round(random.uniform(30000, 120000), 2)
    hire_date = fake.date_this_decade()
    employees.append([employee_id, branch_id, name, position, salary, hire_date])

df_employees = pd.DataFrame(employees, columns=['employee_id', 'branch_id', 'name', 'position', 'salary', 'hire_date'])
df_employees.to_csv('employees.csv', index=False)

# Generate ATMs data
atms = []
for _ in range(num_atms):
    atm_id = fake.uuid4()
    branch_id = random.choice(df_branches['branch_id'])
    location = fake.address()
    status = random.choice(['active', 'maintenance', 'out of service'])
    installation_date = fake.date_this_decade()
    atms.append([atm_id, branch_id, location, status, installation_date])

df_atms = pd.DataFrame(atms, columns=['atm_id', 'branch_id', 'location', 'status', 'installation_date'])
df_atms.to_csv('atms.csv', index=False)

# Generate Investments data
investments = []
for _ in range(num_investments):
    investment_id = fake.uuid4()
    customer_id = random.choice(df_customers['customer_id'])
    investment_type = random.choice(['stocks', 'bonds', 'mutual funds', 'real estate'])
    amount = round(random.uniform(5000, 200000), 2)
    start_date = fake.date_this_decade()
    end_date = fake.date_this_century()
    investments.append([investment_id, customer_id, investment_type, amount, start_date, end_date])

df_investments = pd.DataFrame(investments, columns=['investment_id', 'customer_id', 'investment_type', 'amount', 'start_date', 'end_date'])
df_investments.to_csv('investments.csv', index=False)

# Generate Customer Support data
customer_support = []
for _ in range(num_customer_support):
    interaction_id = fake.uuid4()
    customer_id = random.choice(df_customers['customer_id'])
    employee_id = random.choice(df_employees['employee_id'])
    date = fake.date_this_year()
    issue_type = random.choice(['account issue', 'transaction dispute', 'loan inquiry', 'card issue', 'other'])
    resolution_status = random.choice(['resolved', 'pending', 'escalated'])
    customer_support.append([interaction_id, customer_id, employee_id, date, issue_type, resolution_status])

df_customer_support = pd.DataFrame(customer_support, columns=['interaction_id', 'customer_id', 'employee_id', 'date', 'issue_type', 'resolution_status'])
df_customer_support.to_csv('customer_support.csv', index=False)

print("Data generation complete. CSV files have been saved.")
