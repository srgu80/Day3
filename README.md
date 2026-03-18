#creating warehouse
CREATE OR REPLACE WAREHOUSE my_new_warehouse
WITH 
  WAREHOUSE_SIZE = 'X-SMALL' 
  AUTO_SUSPEND = 300 
  AUTO_RESUME = TRUE 
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse for general data processing';
  
#db creation
CREATE DATABASE IF NOT EXISTS my_new_database
  DATA_RETENTION_TIME_IN_DAYS = 14
  COMMENT = 'Primary database for production data';
  
#table creation
create or replace TABLE TESTDB.PUBLIC.INCIDENTS (
	NUMBER VARCHAR(50),
	SHORT_DESCRIPTION VARCHAR(1000),
	STATE VARCHAR(50),
	PENDING_REASON VARCHAR(1000),
	OPENED TIMESTAMP_NTZ(9),
	OPENED_BY VARCHAR(255),
	ASSIGNMENT_GROUP VARCHAR(255),
	ASSIGNED_TO VARCHAR(255),
	PRIORITY VARCHAR(50)
)COMMENT='Table storing support ticket or incident records'
#refer incidents.csv file


3. Installing Snowflake Connector
Install using pip:
	**pip install snowflake-connector-python**
For Pandas integration:
**pip install "snowflake-connector-python[pandas]"**
Additional packages:
pip install pandas
________________________________________
4. Importing Required Libraries
Basic imports:
import snowflake.connector
For Pandas integration:
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas


Connection :

import snowflake.connector

conn = snowflake.connector.connect(
    user='SRGU80',
    password='setpass',
    
    # The corrected account identifier:
    account='zm35832.me-central2.gcp', 

	https://www.sanfoundry.com/1000-python-questions-answers/
    
    warehouse='COMPUTE_WH',
    database='TESTDB',
    schema='public'
)
print("Connected successfully!")



ROW NUMBER:
create table
CREATE TABLE daily_sales (
    sale_date DATE,
    product VARCHAR(50),
    amount INT
);

INSERT INTO daily_sales (sale_date, product, amount) VALUES
('2024-01-01', 'Laptop', 1200),
('2024-01-02', 'Laptop', 1500),
('2024-01-03', 'Laptop', 900),
('2024-01-04', 'Laptop', 1100),
('2024-01-05', 'Laptop', 1600),
('2024-01-01', 'Mobile', 800),
('2024-01-02', 'Mobile', 950),
('2024-01-03', 'Mobile', 700),
('2024-01-04', 'Mobile', 850),
('2024-01-05', 'Mobile', 1000);
SELECT
    txn_id,
    customer_id,
    txn_date,
    amount,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id, txn_date, amount
        ORDER BY txn_id
    ) AS rn
FROM customer_transactions
ORDER BY customer_id, txn_date, txn_id;

