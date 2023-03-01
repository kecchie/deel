from flask import Flask
import psycopg2
import json
import datetime
from psycopg2.extras import execute_values


app = Flask(__name__)

conn = psycopg2.connect(
    host="db",
    database="mydatabase",
    user="myuser",
    password="mypassword"
)


@app.route('/')
def hello():
    cur = conn.cursor()
    cur.execute("SELECT 1")
    result = cur.fetchone()
    return f'Hello, world! Postgres returned {result[0]}'


if __name__ == '__main__':

    # Create a cursor
    cur = conn.cursor()

    # Define the contract table schema
    contract_table = """CREATE TABLE IF NOT EXISTS contract (
                        contract_id VARCHAR(32) PRIMARY KEY,
                        client_id VARCHAR(32),
                        contract_created_at TIMESTAMP,
                        status VARCHAR(16),
                        completion_date TEXT,
                        is_deleted BOOLEAN,
                        received_at TIMESTAMP,
                        start_date TIMESTAMP,
                        end_date TIMESTAMP,
                        current_flag BOOLEAN
                        );"""

    # Define the invoice table schema
    invoice_table = """CREATE TABLE IF NOT EXISTS invoice (
                        invoice_id VARCHAR(32) PRIMARY KEY,
                        contract_id VARCHAR(32),
                        amount FLOAT,
                        currency VARCHAR(16),
                        is_early_paid BOOLEAN,
                        is_deleted BOOLEAN,
                        received_at TIMESTAMP,
                        start_date TIMESTAMP,
                        end_date TIMESTAMP,
                        current_flag BOOLEAN
                        );"""

    # Create the contract table
    cur.execute(contract_table)

    # Create the invoice table
    cur.execute(invoice_table)

    # Commit the changes
    conn.commit()

    # Open the contracts.json file and load the JSON data
    with open('contracts.json', 'r') as contracts_file:
        contracts_data = json.load(contracts_file)

    # Open the invoices.json file and load the JSON data
    with open('invoices.json', 'r') as invoices_file:
        invoices_data = json.load(invoices_file)

    # Define the batch size
    batch_size = 1000

    # Define the start and end dates for the contracts and invoices
    start_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    end_date = '9999-12-31 23:59:59.999999'

    # Load the contracts into the database
    for i in range(0, len(contracts_data), batch_size):
        contracts_batch = contracts_data[i:i + batch_size]

        # Create a list of tuples containing the contract data
        contract_tuples = [(c['CONTRACT_ID'], c['CLIENT_ID'], c['CONTRACT_CREATED_AT'], c['STATUS'], c['COMPLETION_DATE'],
                            c['IS_DELETED'], c['RECEIVED_AT'], start_date, end_date, True) for c in contracts_batch]

        # Execute the SQL query to insert the contracts
        execute_values(cur, "INSERT INTO contract (contract_id, client_id, contract_created_at, status, completion_date, is_deleted, received_at, start_date, end_date, current_flag) VALUES %s ON CONFLICT (contract_id) DO UPDATE SET current_flag = FALSE, end_date = excluded.start_date - INTERVAL '1 microsecond'", contract_tuples)

        # Commit the changes
        conn.commit()

    # Define the start and end dates for the invoices
    start_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    # Load the invoices into the database
    for i in range(0, len(invoices_data), batch_size):
        invoices_batch = invoices_data[i:i + batch_size]
        conn.commit()

    app.run(debug=True, host='0.0.0.0')
