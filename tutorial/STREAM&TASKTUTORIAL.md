## Streams and Tasks Tutorial

In this tutorial, we’ll be exploring how to construct a database, schema, and table within Snowflake. We’ll also delve into creating a stream and a task, and implementing the concept of Slowly Changing Dimensions (SCD) Type 2 using a stored procedure.

**This tutorial assumes that you have created a trial account within Snowflake and able to confidently navigate Snowflake.**

### Table of Contents

- [Streams and Tasks Tutorial](#streams-and-tasks-tutorial)
  - [Table of Contents](#table-of-contents)
- [Business Case](#business-case)
- [Creating a Database](#creating-a-database)
- [Creating a Schema](#creating-a-schema)
- [Create the Tables](#create-the-tables)
  - [SOURCE\_PRODUCTS Table](#source_products-table)
  - [PRODUCTS Table](#products-table)
- [Create the Stream](#create-the-stream)
- [Create the Procedure](#create-the-procedure)
- [Create a Task](#create-a-task)
- [Testing](#testing)
  - [TEST Table](#test-table)
  - [Insert into SOURCE\_PRODUCTS from TEST](#insert-into-source_products-from-test)
  - [Counts \& Checks](#counts--checks)
- [Clean up and Conclusion](#clean-up-and-conclusion)
  - [Drop Database](#drop-database)
  - [Drop Schema](#drop-schema)
  - [Drop Schema](#drop-schema-1)
  - [Drop Task](#drop-task)
  - [Drop Tables](#drop-tables)
  - [Drop Stored Procedure](#drop-stored-procedure)

## Business Case

Imagine a retail company, “UK Retail Ltd.”, that has a vast product portfolio.......

UK Retail Ltd have been facing challenges in managing their product data due to its volume and complexity. To address this, they decide that there is a business case for the creation of a Snowflake database named **“BRONZE”**.

In **“BRONZE”**, they would like a schema called **“PRODUCTS”** and a table **“SOURCE_PRODUCTS”** to hold the raw product data. 

This data may include product details, prices, suppliers, and more. As the data is updated frequently, they would like a Snowflake stream object to be created called **“STREAM_PRODUCTS”** on the **“SOURCE_PRODUCTS”** table to capture changes in real-time.

The team at UK Retail Ltd would like a table called **“PRODUCTS”** to hold transformed data and believe that using a stored procedure to transform the raw data, and implement changes in product data over time would be beneficial.  

This would allow the team at UK Retail Ltd to maintain a full history of product data and with this setup, UK Retail Ltd will be able to manage their product data more efficiently, leading to better business decisions and improved customer satisfaction.

This tutorial aims to design the transformation and loading components within the Snowflake platform. As part of a proof of concept, we’ll utilise the `products_raw_data.csv` file, which will be updated by us and ingested into Snowflake with changes. 

## Creating a Database

Once you’re logged in, you can create your database named “BRONZE” with the following SQL command:
```sql 
CREATE DATABASE BRONZE
  DATA_RETENTION_TIME_IN_DAYS = 14
  COMMENT = 'This is the Bronze database for raw, unprocessed data.';
;
```
In this example, `DATA_RETENTION_TIME_IN_DAYS` is set to 14 days, which means Snowflake will retain historical data for 14 days. This is useful for ensuring scalability and extensibility. 

The `COMMENT` is used to describe the purpose of the database, which is a good practice for maintainability and clarity.

## Creating a Schema

Before creating a schema, make sure you’re using the correct database. If you want to create the `PRODUCTS` schema in the `BRONZE` database, you would use the following command:

```sql 
USE DATABASE BRONZE;
```
Now you can create the `PRODUCTS` schema with the following command:

```sql 
CREATE SCHEMA IF NOT EXISTS PRODUCTS
  COMMENT = 'This schema contains all the product related tables';
```
To confirm that the schema has been created, you can use the following command to list all schemas in the current database:

```sql 
SHOW SCHEMAS;
```
The `PRODUCTS` schema should appear in the output list.

## Create the Tables

As identified in the business case, there are 2 tables required **SOURCE_PRODUCTS** and **PRODUCTS**, but we will require an additional table to initially load  `products_raw_data.csv` within Snowflake itself, created later.

### SOURCE_PRODUCTS Table

The following command will create the **SOURCE_PRODUCTS** table with commenting using the ``COMMENT`` syntax to support data governance.
```sql
CREATE OR REPLACE TABLE BRONZE.PRODUCTS.SOURCE_PRODUCTS
COMMENT = 'This table stores product data from the "UK Retail Ltd" website.'
( 
    PRODUCT_NAME VARCHAR(16777216) COMMENT 'The name or title of the grocery product.',
    CURRENT_PRICE VARCHAR(16777216) COMMENT 'The current price of the product.',
    PREVIOUS_PRICE VARCHAR(16777216) COMMENT 'The previous price of the product (if applicable).',
    PRICE_PER_EACH VARCHAR(16777216) COMMENT 'The price per unit or each item.',
    CATEGORY VARCHAR(16777216) COMMENT 'The category or department to which the product belongs.',
    PRODUCT_ID VARCHAR(16777216) COMMENT 'A unique identifier for each product.',
    PRODUCT_URL VARCHAR(16777216) COMMENT 'A direct link to the product''s webpage on the "UK Retail Ltd" business.'
);
```

### PRODUCTS Table

The following command will create the **PRODUCTS** table with commenting using the ``COMMENT`` syntax to support data governance.

```sql
CREATE OR REPLACE TABLE BRONZE.PRODUCTS.PRODUCTS
COMMENT = 'This table stores product data from the "UK Retail Ltd" website.'
( 
    PRODUCT_NAME VARCHAR(16777216) COMMENT 'The name or title of the grocery product.',
    CURRENT_PRICE VARCHAR(16777216) COMMENT 'The current price of the product.',
    PREVIOUS_PRICE VARCHAR(16777216) COMMENT 'The previous price of the product (if applicable).',
    PRICE_PER_EACH VARCHAR(16777216) COMMENT 'The price per unit or each item.',
    CATEGORY VARCHAR(16777216) COMMENT 'The category or department to which the product belongs.',
    PRODUCT_ID VARCHAR(16777216) COMMENT 'A unique identifier for each product.',
    PRODUCT_URL VARCHAR(16777216) COMMENT 'A direct link to the product''s webpage on the "UK Retail Ltd" business.', 
  	START_DATE TIMESTAMP_NTZ(9) COMMENT 'The date when the version of the row starts.',
	END_DATE TIMESTAMP_NTZ(9) COMMENT 'The date when the version of the row ends.',
	IS_LATEST BOOLEAN COMMENT 'A boolean flag indicating whether the row is the latest version.'
);
```
The ``START_DATE``, ``END_DATE``, and ``IS_LATEST`` fields have been added to the PRODUCTS table, not the SOURCE_PRODUCTS. This change facilitates the implementation of SCD Type 2, as per business requirements.

## Create the Stream

The following command will create the **STREAM_PRODUCTS** object with commenting using the ``COMMENT`` syntax to support data governance holding information on the Change Data Capture (CDC) from **SOURCE_PRODUCTS**.

```sql
CREATE OR REPLACE STREAM BRONZE.PRODUCTS.STREAM_PRODUCTS
ON TABLE SOURCE_PRODUCTS
COMMENT = 'This stream collects CDC (Change Data Capture) on SOURCE_PRODUCTS.';
```

## Create the Procedure

Before we can create our task, we need to implement the stored procedure **PROC_DIM_PRODUCTS** to implement SCD Type 2 when new data is available within **STREAM_PRODUCTS**. 

The following command will create the **PROC_DIM_PRODUCTS** procedure:

```sql
CREATE OR REPLACE PROCEDURE BRONZE.PRODUCTS.PROC_DIM_PRODUCTS()
    RETURNS VARCHAR
    LANGUAGE SQL EXECUTE
    AS CALLER
    AS $$
    -- First, handle the updated records
    MERGE INTO BRONZE.PRODUCTS.PRODUCTS
    AS target USING BRONZE.PRODUCTS.STREAM_PRODUCTS
    AS source ON target.PRODUCT_ID = source.PRODUCT_ID
    WHEN MATCHED
    AND target.END_DATE IS NULL
    AND source.METADATA$ISUPDATE = TRUE THEN
UPDATE
SET
    target.END_DATE = CURRENT_TIMESTAMP(),
    target.IS_LATEST = FALSE;
-- Then, handle the deleted records
UPDATE
    BRONZE.PRODUCTS.PRODUCTS AS target
SET
    target.END_DATE = CURRENT_TIMESTAMP(),
    target.IS_LATEST = FALSE
WHERE
    EXISTS (
        SELECT
            1
        FROM
            BRONZE.PRODUCTS.STREAM_PRODUCTS AS source
        WHERE
            target.PRODUCT_ID = source.PRODUCT_ID
            AND source.METADATA$ACTION = 'delete'
            AND target.END_DATE IS NULL
    );
-- Finally, handle the inserted records
INSERT INTO
    BRONZE.PRODUCTS.PRODUCTS (
        PRODUCT_ID,
        PRODUCT_NAME,
        CURRENT_PRICE,
        PREVIOUS_PRICE,
        PRICE_PER_EACH,
        CATEGORY,
        PRODUCT_URL,
        START_DATE,
        END_DATE,
        IS_LATEST
    )
SELECT
    source.PRODUCT_ID,
    source.PRODUCT_NAME,
    source.CURRENT_PRICE,
    source.PREVIOUS_PRICE,
    source.PRICE_PER_EACH,
    source.CATEGORY,
    source.PRODUCT_URL,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
FROM
    BRONZE.PRODUCTS.STREAM_PRODUCTS AS source
WHERE
    source.METADATA$ACTION = 'insert'
    AND NOT EXISTS (
        SELECT
            1
        FROM
            BRONZE.PRODUCTS.PRODUCTS AS target
        WHERE
            target.PRODUCT_ID = source.PRODUCT_ID
            AND target.END_DATE IS NULL
    );
RETURN 'Procedure PROC_DIM_PRODUCTS executed successfully';
$$;
```

## Create a Task

The following command will create the **TASK_PRODUCTS** object with commenting using the ``COMMENT`` syntax to support data governance. 

```sql
CREATE OR REPLACE TASK TASK_PRODUCTS
COMMENT = 'This task runs PROC_DIM_PRODUCTS on an agreed schedule'
WAREHOUSE = COMPUTE_WH SCHEDULE = '5 minute'
WHEN SYSTEM$STREAM_HAS_DATA('STREAM_PRODUCTS') AS CALL PROC_DIM_PRODUCTS();
```

The **TASK_PRODUCTS** will check for new records added to **STREAM_PRODUCTS** every 5 minutes and be used as a trigger to execute a stored procedure called **PROC_DIM_PRODUCTS** . 

The following command will ``RESUME`` **TASK_PRODUCTS** :

```sql 
ALTER TASK TASK_LOAD_ORACLE_PRODUCTS RESUME;
```

By default Snowflake does not activate tasks straight away and if left running may incur costs associated with your trial account.

## Testing

### TEST Table

**Import data manually using Snowsight interface, using the below conditions:**

```sql 
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=TRUE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
)
ON_ERROR=CONTINUE
PURGE=TRUE
```

### Insert into SOURCE_PRODUCTS from TEST

The following command will insert data from **TEST Table** into **SOURCE_PRODUCT**:
```sql 
INSERT INTO
    BRONZE.PRODUCTS.SOURCE_PRODUCT
SELECT
    NAME,
    CURRENT_PRICE,
    PREVIOUS_PRICE,
    CATEGORY,
    PRODUCT_ID,
    PRODUCT_URL
FROM
    BRONZE.PRODUCTS.TEST;
```
***Please note, the stream object **STREAM_PRODUCTS** will not populate the **PRODUCTS** table until **TASK_PRODUCTS** has elapsed 5 minutes or the duration you’ve set in `SCHEDULE` clause.***

### Counts & Checks

Here are some tests you can run to check if the procedure  `PROC_DIM_PRODUCTS`  is working as expected:

1.  **Test for Updated Records:**
    
    -   Insert a record into  **STREAM_PRODUCTS**  with  `METADATA$ISUPDATE = TRUE`  and a  `PRODUCT_ID`  that matches an existing record in  **PRODUCTS**  where  `END_DATE IS NULL`.
    
    -   Run the procedure.
    
    -   Check if the corresponding record in  **PRODUCTS**  has  `END_DATE`  set to the current timestamp and  `IS_LATEST`  set to  `FALSE`.
    
2.  **Test for Deleted Records:**
    
    -   Insert a record into  **STREAM_PRODUCTS**  with  `METADATA$ACTION = 'delete'`  and a  `PRODUCT_ID`  that matches an existing record in  **PRODUCTS**  where  `END_DATE IS NULL`.
   
    -   Run the procedure.
    
    -   Check if the corresponding record in  **PRODUCTS**  has  `END_DATE`  set to the current timestamp and  `IS_LATEST`  set to  `FALSE`.
   
3.  **Test for Inserted Records:**
    
    -   Insert a record into  **STREAM_PRODUCTS**  with  `METADATA$ACTION = 'insert'`  and a  `PRODUCT_ID`  that does not match any existing record in  **PRODUCTS**  where  `END_DATE IS NULL`.
    
    -   Run the procedure.
    
    -   Check if a new record is inserted into  **PRODUCTS**  with the same values as in  **STREAM_PRODUCTS**,  `START_DATE`  set to the current timestamp,  `END_DATE`  set to  `NULL`, and  `IS_LATEST`  set to  `TRUE`.
    
4.  **Test for Procedure’s Return Message:**
    
    -   Run the procedure and check if it returns  `'Procedure PROC_DIM_PRODUCTS executed successfully'`.

The below commands can be used to check and compare as you add new data into **TEST**:

```sql 
-- Completes a select all on STREAM_PRODUCTS.
SELECT
    *
FROM
    STREAM_PRODUCTS;
-- Completes a count of rows in STREAM_PRODUCTS.
SELECT
    COUNT(*)
FROM
    STREAM_PRODUCTS;
-- Completes a count of rows in TEST table.
SELECT
    COUNT(*)
FROM
    BRONZE.PRODUCTS.TEST;
    -- Completes a count of rows in SOURCE_PRODUCTS
SELECT
    COUNT(*)
FROM
    BRONZE.PRODUCTS.SOURCE_PRODUCTS;
    -- Completes a count of rows in PRODUCTS
SELECT
    COUNT(*)
FROM
    BRONZE.PRODUCTS.PRODUCTS;
```
**You can update and change values within ``products_raw_data.csv`` as you wish.**

## Clean up and Conclusion

**Once completed with the tutorial**, to ensure that you do not incur costs, we must run the below commands:

### Drop Database

```sql 
-- Drop the database
DROP DATABASE IF EXISTS BRONZE;
-- Check if the database has been dropped
SELECT
    *
FROM
    INFORMATION_SCHEMA.DATABASES
WHERE
    DATABASE_NAME = 'BRONZE';
```
### Drop Schema

```sql
-- Drop the schema
    DROP SCHEMA IF EXISTS PRODUCTS;
-- Check if the schema has been dropped
SELECT
    *
FROM
    INFORMATION_SCHEMA.SCHEMATA
WHERE
    SCHEMA_NAME = 'PRODUCTS';
```

### Drop Schema

```sql 
-- Drop the stream
    DROP STREAM IF EXISTS STREAM_PRODUCTS;
-- Check if the stream has been dropped
    SHOW STREAMS LIKE 'STREAM_PRODUCTS';
```

### Drop Task

```sql 
-- Drop the task
    DROP TASK IF EXISTS TASK_PRODUCTS;
-- Check if the task has been dropped
    SHOW TASKS LIKE 'TASK_PRODUCTS';
```

### Drop Tables

```sql
-- Drop the tables
    DROP TABLE IF EXISTS TEST;
DROP TABLE IF EXISTS SOURCE_PRODUCT;
DROP TABLE IF EXISTS PRODUCT;
-- Check if the tables have been dropped
SELECT
    *
FROM
    INFORMATION_SCHEMA.TABLES
WHERE
    TABLE_NAME IN ('TEST', 'SOURCE_PRODUCT', 'PRODUCT');
```

### Drop Stored Procedure 

```sql 
-- Drop the stored procedure
    DROP PROCEDURE IF EXISTS PROC_DIM_PRODUCTS;
-- Check if the stored procedure has been dropped
    SHOW PROCEDURES LIKE 'PROC_DIM_PRODUCTS';
```
