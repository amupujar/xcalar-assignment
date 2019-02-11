Name - Amruta Pujar
How to run?
In folder xcalar
Python script i.e test.py runs all the queries to get the output
init.sql-should be in same folder (creates table schema, creates relationship)
Dockerfile- couldnt resolve the issues
requirements.txt- gives the details of python virtual environment
setup.sh

test.py-main python script that uses python client to connect with a postgres instance, insert mock data and run queries
Note* The script needs an instance of postgres running on host machine on the port (5432) that has been hardcoded. Username, database name
and password attributes have also been hardcoded as I couldn't get python client to interact with docker instance of postgres.
To run this need to have init.sql file placed in same folder.
In main function first connection is established using psycopg2.
It is a database adapter for python programming.
Databse name - postgres and other values provided is hardcoded to get the connection.
Faker object is created to generate the randomm data and for no of customers , limit is set to 1000 and no of accounts is set to 10 while running the query.
SQl queries - is ran over ODBC from test.py

Dockerfile- is not created and couldnt resolve issues encountered.

Able to run docker image and create database in docker but unable to sping up using python client due to TCP connections issues.



