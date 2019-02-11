import psycopg2
from faker import Faker
from random import randint
import random
import datetime
import os

init_schema_script = str(os.path.dirname(os.path.realpath(__file__)) + "/init.sql")

top_10_states_with_highest_cust_bal_query = "select cs.statecode as state_code, sum(ac.balance) as net_balance from customer_account_map AS map INNER JOIN accounts AS ac ON map.account_id = ac.account_id INNER JOIN customers as cs ON map.customer_id = cs.customer_id group by cs.statecode order by net_balance desc limit 10;"
top_10_lowest_customer_balances = "select cs.customer_id as ac_id, sum(ac.balance) as net_balance from customer_account_map AS map INNER JOIN accounts AS ac ON map.account_id = ac.account_id INNER JOIN customers as cs ON map.customer_id = cs.customer_id group by cs.customer_id order by net_balance asc limit 10;"
top_10_highest_customer_balances = "select cs.customer_id as ac_id, sum(ac.balance) as net_balance from customer_account_map AS map INNER JOIN accounts AS ac ON map.account_id = ac.account_id INNER JOIN customers as cs ON map.customer_id = cs.customer_id group by cs.customer_id order by net_balance desc limit 10;"

'''
Drops tables and recreates schema
as a part of test setup
'''
def init_schema(connection):

    cursor = connection.cursor()
    with open(init_schema_script, 'r') as schema_file:
        for line in schema_file:
            try:
                if(line is None or line == '\n' or len(line) < 1):
                    continue
                else:
                    cursor.execute(line)
            except Exception:
                # Swallowing any exceptions
                pass

def load_data(connection, number_of_customers=1000, accts_per_customer=10):
    fakerobj = Faker()
    cursor = connection.cursor()
    for customer_index in range (0, number_of_customers):
        try:
            name = fakerobj.name().split(" ")
            address = fakerobj.address().split(",")
            zip_code = address[1].split(" ")[2]
            state_code = address[1].split(" ")[1]
            customer_id  = random.randint(10000, 99999)
            
            cursor.execute('INSERT INTO CUSTOMERS VALUES(%s, %s, %s, %s, %s, %s, %s, %s)', (customer_id, name[0].strip(), name[1].strip(), address[0].strip(), fakerobj.city().strip(), state_code.strip(), zip_code.strip(), datetime.datetime.now()))
        
            for acct_index in range(0, accts_per_customer):
                acct_id = random.randint(1000000,9999999)
                balance = random.uniform(500, 2000)
                cursor.execute('INSERT INTO ACCOUNTS VALUES(%s,%s,%s)', (acct_id, balance, datetime.datetime.now()))

                cursor.execute('INSERT INTO CUSTOMER_ACCOUNT_MAP VALUES(%s,%s)', (customer_id, acct_id))

        except Exception, ex:
            #Swallow exceptions
            pass
    return

'''
This method displays the top 10 states that
have the highest net customer balances
'''
def display_top_10_states_with_highest_cust_balances(connection):
    cursor = connection.cursor()
    cursor.execute(top_10_states_with_highest_cust_bal_query)
    rows = cursor.fetchall()

    print("Displaying top 10 states with highest customer account balances..\n")
    for row in rows:
        print row[0]
    print("\n")

'''
This method displays the top 10 customers with
lowest account balances
'''
def get_top_10_customers_with_lowest_balances(connection, display=True):
    cursor = connection.cursor()
    cursor.execute(top_10_lowest_customer_balances)
    rows = cursor.fetchall()

    dataRows = []

    if(display):
        print("Displaying top 10 customers with lowest account balances..\n")

    for row in rows:
        if(display):
            print row[0]
        dataRows.append(row)
    print("\n")
    return dataRows

'''
This method displays top 10 customers that have
the highest account balances
'''
def get_top_10_customers_with_highest_account_balances(connection, display=True):
    cursor = connection.cursor()
    cursor.execute(top_10_highest_customer_balances)
    rows = cursor.fetchall()

    dataRows = []

    if(display):
        print("Displaying top 10 customers with highest account balances..\n")

    for row in rows:
        if(display):
            print row[0]
        dataRows.append(row)
    print("\n")
    return dataRows

'''
deducts 10% balance from the a/c of mentioned customer
'''
def deductBal(connection , dataRow):
    amountToDeduct = 0.10 * float(dataRow[1])

    cursor = connection.cursor()
    cursor.execute('select map.account_id from customer_account_map AS map INNER JOIN accounts as ac on map.account_id = ac.account_id where ac.balance > (%s) and map.customer_id=(%s)', (amountToDeduct, dataRow[0]))
    row = cursor.fetchall()
    if(len(row) > 0):
        cursor.execute('UPDATE accounts SET balance = balance - (%s) WHERE account_id=(%s)', (amountToDeduct, row[0][0]))
        return amountToDeduct
    return 0

'''
Adds 10% deducted from top a/c balance holder to
low balance holder
'''
def addbal(connection, customer_id, balToAdd):
    cursor = connection.cursor()
    cursor.execute('select map.account_id from customer_account_map AS map INNER JOIN accounts as ac on map.account_id = ac.account_id where map.customer_id=' + str(customer_id))
    row = cursor.fetchall()
    cursor.execute('UPDATE accounts SET balance = balance + (%s) WHERE account_id=(%s)', (balToAdd, row[0][0]))

'''
This method transfers 10% balance from top 10
customer a/c balances and moves it to the bottom 10
customer a/c balance
'''
def operation_robinhood(connection):
    topTenHighBalCust = get_top_10_customers_with_highest_account_balances(connection, False)
    topTenLowBalCust = get_top_10_customers_with_lowest_balances(connection, False)

    print("Starting operation robinhood to move 10% balance from highest balance holders to lowest balance holders..\n")
    for row in zip(topTenHighBalCust, topTenLowBalCust):
        bal = deductBal(connection, row[0])
        addbal(connection, row[1][0], bal)
    print("Transfer completed\n")


if __name__ == "__main__":
    connection = psycopg2.connect("dbname='postgres' user='postgres' host='127.0.0.1' port='5432' password='123'")
    connection.autocommit = True
    init_schema(connection)
    load_data(connection)
    display_top_10_states_with_highest_cust_balances(connection)
    get_top_10_customers_with_lowest_balances(connection)
    get_top_10_customers_with_highest_account_balances(connection)
    operation_robinhood(connection)
    connection.close()