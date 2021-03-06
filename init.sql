DROP TABLE IF EXISTS CUSTOEMRS;
DROP TABLE IF EXISTS ACCOUNTS;
DROP TABLE IF EXISTS CUSTOMER_ACCOUNT_MAP

CREATE TABLE IF NOT EXISTS CUSTOMERS(CUSTOMER_ID int PRIMARY KEY, FIRST_NAME VARCHAR, LAST_NAME VARCHAR, STREET_ADDRESS TEXT, CITY VARCHAR, STATECODE VARCHAR(2), ZIP varchar(5), CREATE_DATE TIMESTAMP);

CREATE TABLE IF NOT EXISTS ACCOUNTS(ACCOUNT_ID long PRIMARY KEY, BALANCE FLOAT, CREATE_DATE TIMESTAMP);

CREATE TABLE IF NOT EXISTS CUSTOMER_ACCOUNT_MAP(CUSTOMER_ID int, ACCOUNT_ID int);

ALTER TABLE CUSTOMER_ACCOUNT_MAP ADD CONSTRAINT FK_CUST_ID FOREIGN KEY(CUSTOMER_ID) REFERENCES CUSTOMERS(CUSTOMER_ID);
ALTER TABLE CUSTOMER_ACCOUNT_MAP ADD CONSTRAINT FK_ACCT_ID FOREIGN KEY(ACCOUNT_ID) REFERENCES ACCOUNTS(ACCOUNT_ID);
