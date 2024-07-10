from flask import Flask, render_template
import pandas as pd

app = Flask(__name__)

# Load the data from CSV files
customers_df = pd.read_csv('/home/kali/Desktop/projects/banking _project/data_prepare/customers_today.csv')
accounts_df = pd.read_csv('/home/kali/Desktop/projects/banking _project/data_prepare/accounts_today.csv')
transactions_df = pd.read_csv('/home/kali/Desktop/projects/banking _project/data_prepare/transactions_today.csv')
loans_df = pd.read_csv('/home/kali/Desktop/projects/banking _project/data_prepare/loans_today.csv')
cards_df = pd.read_csv('/home/kali/Desktop/projects/banking _project/data_prepare/cards_today.csv')
branches_df = pd.read_csv('/home/kali/Desktop/projects/banking _project/data_prepare/branches_today.csv')
employees_df = pd.read_csv('/home/kali/Desktop/projects/banking _project/data_prepare/employees_today.csv')

@app.route('/')
def index():
    # Convert the DataFrames to HTML tables
    customers_html = customers_df.to_html(index=False)
    accounts_html = accounts_df.to_html(index=False)
    transactions_html = transactions_df.to_html(index=False)
    loans_html = loans_df.to_html(index=False)
    cards_html = cards_df.to_html(index=False)
    branches_html = branches_df.to_html(index=False)
    employees_html = employees_df.to_html(index=False)
    
    # Render the index.html template with the data
    return render_template(
        'index.html',
        customers_html=customers_html,
        accounts_html=accounts_html,
        transactions_html=transactions_html,
        loans_html=loans_html,
        cards_html=cards_html,
        branches_html=branches_html,
        employees_html=employees_html
    )

if __name__ == '__main__':
    app.run(debug=True)
