from flask import Flask, render_template
import mysql.connector

app = Flask(__name__)
# MySQL Database Configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'bank_raw_layer'  # Replace with your database name
}
# Function to fetch data from MySQL using mysql.connector
def fetch_data(table_name):
    query = f'SELECT * FROM {table_name}'
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    connection.close()
    return data

# List of tables to fetch data from
tables = [
    'dlt_accounts', 'dlt_atms', 'dlt_bill_payments', 'dlt_branches', 'dlt_cards',
    'dlt_cheques', 'dlt_credit_scores', 'dlt_customer_support', 'dlt_customers',
    'dlt_employees', 'dlt_fixed_deposits', 'dlt_insurance', 'dlt_investments',
    'dlt_loans', 'dlt_mortgage_applications', 'dlt_online_banking',
    'dlt_recurring_deposits', 'dlt_savings_goals', 'dlt_service_charges',
    'dlt_transactions'
]

# Example route to render the dashboard
@app.route('/')
def dashboard():
    # Dictionary to store data for each table
    table_data = {}

    # Fetch data for each table
    for table in tables:
        table_data[table] = fetch_data(table)

    # Render template with data
    return render_template('index.html', table_data=table_data, tables=tables)

if __name__ == '__main__':
    app.run(debug=True)