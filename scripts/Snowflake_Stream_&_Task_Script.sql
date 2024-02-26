-- Creating a Database
CREATE DATABASE BRONZE DATA_RETENTION_TIME_IN_DAYS = 14 COMMENT = 'This is the Bronze database for raw, unprocessed data.';
-- Creating a Schema
USE DATABASE BRONZE;
CREATE SCHEMA IF NOT EXISTS PRODUCTS COMMENT = 'This schema contains all the product related tables';
-- Listing Schemas
SHOW SCHEMAS;
-- Create the Tables
-- Test Table (Import data manually using Snowsight interface)
-- Including the below conditions:
-- FILE_FORMAT = (
--     TYPE=CSV,
--     SKIP_HEADER=1,
--     FIELD_DELIMITER=',',
--     TRIM_SPACE=TRUE,
--     FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
--     REPLACE_INVALID_CHARACTERS=TRUE,
--     DATE_FORMAT=AUTO,
--     TIME_FORMAT=AUTO,
--     TIMESTAMP_FORMAT=AUTO
-- )
-- ON_ERROR=CONTINUE
-- PURGE=TRUE
-- SOURCE_PRODUCTS Table
CREATE
OR REPLACE TABLE BRONZE.PRODUCTS.SOURCE_PRODUCTS COMMENT = 'This table stores product data from the "UK Retail Ltd" website.' (
    PRODUCT_NAME VARCHAR(16777216) COMMENT 'The name or title of the grocery product.',
    CURRENT_PRICE VARCHAR(16777216) COMMENT 'The current price of the product.',
    PREVIOUS_PRICE VARCHAR(16777216) COMMENT 'The previous price of the product (if applicable).',
    PRICE_PER_EACH VARCHAR(16777216) COMMENT 'The price per unit or each item.',
    CATEGORY VARCHAR(16777216) COMMENT 'The category or department to which the product belongs.',
    PRODUCT_ID VARCHAR(16777216) COMMENT 'A unique identifier for each product.',
    PRODUCT_URL VARCHAR(16777216) COMMENT 'A direct link to the product''s webpage on the "UK Retail Ltd" business.'
);
-- PRODUCTS Table
CREATE
OR REPLACE TABLE BRONZE.PRODUCTS.PRODUCTS COMMENT = 'This table stores product data from the "UK Retail Ltd" website.' (
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
-- Creating the Stream
CREATE OR REPLACE STREAM BRONZE.PRODUCTS.STREAM_PRODUCTS COMMENT = 'This stream collects CDC (Change Data Capture) on SOURCE_PRODUCTS.' ON TABLE SOURCE_PRODUCTS;
-- Create the Procedure
CREATE OR REPLACE PROCEDURE BRONZE.PRODUCTS.PROC_DIM_PRODUCTS()
    RETURNS VARCHAR
    LANGUAGE SQL EXECUTE
    AS CALLER
    AS $$ -- First, handle the updated records
    BEGIN
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
    END;
$$;
-- Creating a Task
    CREATE
    OR REPLACE TASK TASK_PRODUCTS COMMENT = 'This task runs PROC_DIM_PRODUCTS on an agreed schedule' WAREHOUSE = COMPUTE_WH SCHEDULE = '5 minute'
    WHEN SYSTEM$STREAM_HAS_DATA('STREAM_PRODUCTS') AS CALL PROC_DIM_PRODUCTS();
-- Resuming the Task
    ALTER TASK TASK_PRODUCTS RESUME;
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
-- Drop the database
    DROP DATABASE IF EXISTS BRONZE;
-- Check if the database has been dropped
SELECT
    *
FROM
    INFORMATION_SCHEMA.DATABASES
WHERE
    DATABASE_NAME = 'BRONZE';
-- Drop the schema
    DROP SCHEMA IF EXISTS PRODUCTS;
-- Check if the schema has been dropped
SELECT
    *
FROM
    INFORMATION_SCHEMA.SCHEMATA
WHERE
    SCHEMA_NAME = 'PRODUCTS';
-- Drop the stream
    DROP STREAM IF EXISTS STREAM_PRODUCTS;
-- Check if the stream has been dropped
    SHOW STREAMS LIKE 'STREAM_PRODUCTS';
-- Drop the task
    DROP TASK IF EXISTS TASK_PRODUCTS;
-- Check if the task has been dropped
    SHOW TASKS LIKE 'TASK_PRODUCTS';
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
-- Drop the stored procedure
    DROP PROCEDURE IF EXISTS PROC_DIM_PRODUCTS;
-- Check if the stored procedure has been dropped
    SHOW PROCEDURES LIKE 'PROC_DIM_PRODUCTS';