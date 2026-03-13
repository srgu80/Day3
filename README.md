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
    
    warehouse='COMPUTE_WH',
    database='TESTDB',
    schema='public'
)
print("Connected successfully!")

