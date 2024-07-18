from flask import Flask, render_template
import mysql.connector

app = Flask(__name__)

def get_db_connection():
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='root',
        database='bank_raw_layer'
    )
    return connection

@app.route('/')
def index():
    tables = [
        ('dlt_accounts', 'dlt_accounts'),
        ('dlt_atms', 'dlt_atms'),
        ('dlt_bill_payments', 'dlt_bill_payments'),
        ('dlt_branches', 'dlt_branches'),
        ('dlt_cards', 'dlt_cards'),
        ('dlt_cheques', 'dlt_cheques'),
        ('dlt_credit_scores', 'dlt_credit_scores'),
        ('dlt_customer_support', 'dlt_customer_support'),
        ('dlt_customers', 'dlt_customers'),
        ('dlt_employees', 'dlt_employees'),
        ('dlt_fixed_deposits', 'dlt_fixed_deposits'),
        ('dlt_insurance', 'dlt_insurance'),
        ('dlt_investments', 'dlt_investments'),
        ('dlt_loans', 'dlt_loans'),
        ('dlt_mortgage_applications', 'dlt_mortgage_applications'),
        ('dlt_online_banking', 'dlt_online_banking'),
        ('dlt_recurring_deposits', 'dlt_recurring_deposits'),
        ('dlt_savings_goals', 'dlt_savings_goals'),
        ('dlt_service_charges', 'dlt_service_charges'),
        ('dlt_transactions', 'dlt_transactions')
    ]
    return render_template('index.html', tables=tables)

@app.route('/table/<table_name>')
def show_table(table_name):
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    cursor.execute(f'SELECT * FROM {table_name}')
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    return render_template('table.html', table_name=table_name, rows=rows)

if __name__ == '__main__':
    app.run(debug=True)
