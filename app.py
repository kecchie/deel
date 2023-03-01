from flask import Flask
import psycopg2
import json

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
    print('invoices')
    # Open the JSON file and parse its contents as a list of dictionaries
    with open("invoices.json", "r") as f:
        invoices = json.load(f)
    print(invoices)
    app.run(debug=True, host='0.0.0.0')
