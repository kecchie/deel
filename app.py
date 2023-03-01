from flask import Flask
import psycopg2
import json
import datetime


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

    # Open the JSON file and parse its contents as a list of dictionaries
    # Create the contracts table
    print('1')
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE testing (
        contract_id TEXT PRIMARY KEY,
        client_id TEXT
    );
    """)

    # cur.execute("""
    # CREATE TABLE IF NOT EXISTS testing (
    #     contract_id TEXT PRIMARY KEY,
    #     client_id TEXT,
    #     contract_created_at TIMESTAMP,
    #     status TEXT,
    #     completion_date TIMESTAMP,
    #     is_deleted BOOLEAN,
    #     received_at TIMESTAMP
    # );
    # """)

    print('2')
    # # Create the invoices table
    # cur.execute("""
    # CREATE TABLE IF NOT EXISTS invoices (
    #     invoice_id TEXT PRIMARY KEY,
    #     contract_id TEXT REFERENCES contracts (contract_id),
    #     amount NUMERIC(10,2),
    #     currency TEXT,
    #     is_early_paid BOOLEAN,
    #     is_deleted BOOLEAN,
    #     received_at TIMESTAMP
    # )
    # """)

    # # Load the contracts from the JSON file and insert them into the contracts table
    # with open('contracts.json', 'r') as f:
    #     contracts_data = json.load(f)
    #     contracts = [(c['CONTRACT_ID'], c['CLIENT_ID'], c['CONTRACT_CREATED_AT'], c['STATUS'],
    #                   c['COMPLETION_DATE'], c['IS_DELETED'], c['RECEIVED_AT']) for c in contracts_data]
    # print('3')
    # batch_size = 1000
    # for i in range(0, len(contracts), batch_size):
    #     cur.executemany("""
    #         INSERT INTO contracts (contract_id, client_id, contract_created_at, status, completion_date, is_deleted, received_at)
    #         VALUES (%s, %s, %s, %s, %s, %s, %s)
    #         ON CONFLICT (contract_id) DO UPDATE SET
    #             client_id = excluded.client_id,
    #             contract_created_at = excluded.contract_created_at,
    #             status = excluded.status,
    #             completion_date = excluded.completion_date,
    #             is_deleted = excluded.is_deleted,
    #             received_at = excluded.received_at
    #     """, contracts[i:i+batch_size])
    #     conn.commit()
    # print('4')
    # # Load the invoices from the JSON file and insert them into the invoices table
    # with open('invoices.json', 'r') as f:
    #     invoices_data = json.load(f)
    #     invoices = [(i['INVOICE_ID'], i['CONTRACT_ID'], i['AMOUNT'], i['CURRENCY'],
    #                 i['IS_EARLY_PAID'], i['IS_DELETED'], i['RECEIVED_AT']) for i in invoices_data]
    # print('5')
    # for i in range(0, len(invoices), batch_size):
    #     cur.executemany("""
    #         INSERT INTO invoices (invoice_id, contract_id, amount, currency, is_early_paid, is_deleted, received_at)
    #         VALUES (%s, %s, %s, %s, %s, %s, %s)
    #         ON CONFLICT (invoice_id) DO UPDATE SET
    #             contract_id = excluded.contract_id,
    #             amount = excluded.amount,
    #             currency = excluded.currency,
    #             is_early_paid = excluded.is_early_paid,
    #             is_deleted = excluded.is_deleted,
    #             received_at = excluded.received_at
    #     """, invoices[i:i+batch_size])
    #     conn.commit()
    #     print('6')

    # # Close the database connection
    # cur.close()
    # conn.close()
    # print('7')

    # #app.run(debug=True, host='0.0.0.0')
