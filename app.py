import psycopg2
import json
from datetime import datetime
from psycopg2.extras import execute_values
import uuid

conn = psycopg2.connect(
    host="db",
    database="deeldatabase",
    user="myuser",
    password="mypassword"
)

if __name__ == '__main__':

    # Create a cursor
    cur = conn.cursor()

    cur.execute('DROP TABLE IF EXISTS contracts CASCADE')
    cur.execute('DROP TABLE IF EXISTS invoices CASCADE')

    # Define the contract table schema
    contract_table = """CREATE TABLE contracts (
                        id VARCHAR(64) PRIMARY KEY,
                        contract_id VARCHAR(32),
                        client_id VARCHAR(32),
                        contract_created_at TIMESTAMP,
                        status VARCHAR(32),
                        completion_date TEXT NULL,
                        is_deleted BOOLEAN,
                        received_at TIMESTAMP,
                        start_date TIMESTAMP,
                        end_date TIMESTAMP,
                        is_current BOOLEAN
                        );"""

    # Create the contract table
    cur.execute(contract_table)
    conn.commit()

    # Define the invoice table schema
    invoice_table = """CREATE TABLE invoices (
                        id VARCHAR(64) PRIMARY KEY,
                        invoice_id VARCHAR(32),
                        contract_id VARCHAR(32),
                        amount FLOAT,
                        currency VARCHAR(16),
                        is_early_paid BOOLEAN,
                        is_deleted BOOLEAN,
                        received_at TIMESTAMP
                        --FOREIGN KEY (CONTRACT_ID) REFERENCES contracts (ID)
                        );"""

    # Create the invoice table
    cur.execute(invoice_table)

    conn.commit()

    # --------------------------------------------------------------
    # Contracts
    # Open the contracts.json file and load the JSON data
    with open('contracts.json', 'r') as contracts_file:
        contracts_data = json.load(contracts_file)

    # Insert statement for contracts
    sql_insert = """INSERT INTO contracts (
                        id,
                        contract_id,
                        client_id,
                        contract_created_at,
                        status,
                        completion_date,
                        is_deleted,
                        received_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """

    contract_columns = ['ID',
                        'CONTRACT_ID',
                        'CLIENT_ID',
                        'CONTRACT_CREATED_AT',
                        'STATUS',
                        'COMPLETION_DATE',
                        'IS_DELETED',
                        'RECEIVED_AT',
                        'START_DATE',
                        'END_DATE',
                        'IS_CURRENT']

    scd2_fields = ['contract_id']

    # Iterate through your contracts_data list
    for contract_row in contracts_data:
        # Define the start and end dates for the contracts and invoices
        start_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        end_date = '9999-12-31 23:59:59.999999'
        contract_row_with_id = contract_row
        contract_row_with_id["ID"] = str(uuid.uuid4())

        cur.execute(
            f"SELECT * FROM contracts WHERE contract_id='{contract_row['CONTRACT_ID']}' AND end_date IS NULL")
        existing_record = cur.fetchone()

        if existing_record:
            cur.execute(
                f"UPDATE contracts SET end_date='{start_date}', is_current=false  WHERE id='{existing_record[0]}'")
            conn.commit()

        # Insert the new record with a null expiry_date
        insert_data = []
        for column in contract_columns:
            if column == 'START_DATE':
                insert_data.append(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
            elif column == 'END_DATE':
                insert_data.append(None)
            elif column == 'IS_CURRENT':
                insert_data.append(True)
            elif column in scd2_fields and existing_record:
                insert_data.append(
                    existing_record[contract_columns.index(column)])
            else:
                insert_data.append(contract_row[column])

        cur.execute(
            f"INSERT INTO contracts ({', '.join(contract_columns)}) VALUES ({', '.join(['%s']*len(contract_columns))})", insert_data)
        conn.commit()

    # --------------------------------------------------------------
    # Invoices
    # Open the invoices.json file and load the JSON data
    with open('invoices.json', 'r') as invoices_file:
        invoices_data = json.load(invoices_file)

    batch_size = 10

    for i in range(0, len(invoices_data), batch_size):
        invoices_batch = invoices_data[i:i + batch_size]

        # Create a list of tuples containing the contract data
        invoice_tuples = [(str(uuid.uuid4()), invoices['INVOICE_ID'], invoices['CONTRACT_ID'], invoices['AMOUNT'], invoices['CURRENCY'], invoices['IS_EARLY_PAID'],
                           invoices['IS_DELETED'], invoices['RECEIVED_AT']) for invoices in invoices_batch]

        # Execute the SQL query to insert the invoices
        execute_values(
            cur, "INSERT INTO invoices (id, invoice_id, contract_id, amount, currency, is_early_paid, is_deleted, received_at) VALUES %s", invoice_tuples)

        # Commit the changes
        conn.commit()

    print('done')
