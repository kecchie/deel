# Deel Assignment

This script imports contract and invoice data from JSON files and saves them into PostgreSQL database tables.

## Requirements
- Docker
- Docker Compose

## Used packages
- psycopg2
- json
- datetime
- uuid

## Running 
To spin up the database and run the script use the following command.
```
docker-compose up --build
```

## How it works?

We describe two docker files that can be started together using Docker compose. First Docker container runs our database and accessible over port 5388 and the second container runs our python script.

Pyhton script does the following:
First, it creates a databse connection using the credentials.
Then, it defines the contract and inovoice table schemas and creates empty tables in Postgres database. Next step reads both JSON files and then using SCD2 inserts all the rows from the JSON files into the empty tables while invalidating older entries.

SCD2 fileds to see the updated fields and older versions are;
- START_DATE
- END_DATE
- IS_CURRENT
