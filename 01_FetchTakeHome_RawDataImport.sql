/***********************************************
Name:           Deirdre Pethokoukis
Script Name:    01_FetchTakeHome_RawDataImport
Database:       USE FetchTakeHome;
Purpose:        Import Raw Tables
                    -Transactions
                    -Users
                    -Products
*************************************************/
---------------------
-- DATABASE SET UP --
---------------------

--1.Create Database for project
    CREATE DATABASE FetchTakeHome;


--2. Specify database to use for script
    USE FetchTakeHome;

------------------
-- UPLOAD DATA  --
------------------

--1. Transactions Table

    --a. Create Table 
    --set all columns to nvarchar(255) to avoid losing information by immediate conversion to non-string columns
    -- DROP TABLE raw_transaction_table ;
    CREATE TABLE raw_transaction_table (
          RECEIPT_ID        nvarchar(255)
        , PURCHASE_DATE     nvarchar(255)
        , SCAN_DATE         nvarchar(255)
        , STORE_NAME        nvarchar(255)
        , USER_ID           nvarchar(255)
        , BARCODE           nvarchar(255)
        , FINAL_QUANTITY    nvarchar(255)
        , FINAL_SALE        nvarchar(255)
        )

    --b. Bulk Insert table in SQL
    --specify arguments that are consistent for a CSV file: 
    BULK INSERT raw_transaction_table 
    FROM '/var/opt/mssql/data/TRANSACTION_TAKEHOME.csv'
    WITH (
        DATAFILETYPE = 'char'
        , FORMAT = 'CSV'
        , FIELDTERMINATOR = ','     --column delimitor
        , ROWTERMINATOR = '\n'      --row terminator
        , FIRSTROW = 2              --allows first row to be headers
       -- , CODEPAGE = '65001'      --Unable to specify bulk insert codepage in Linux, i.e. the way I am accessing SQL Server.
    )
    --(50000 rows affected)
    --NOTE: suspicous that exactly 50,000 rows -- is this all of the data?

    SELECT      TOP (100) *
    FROM        raw_transaction_table 

    --c. Basic QC Check

        --i. Count of rows matches CSV
            SELECT      COUNT(1) AS [TotalRows]
            FROM        raw_transaction_table 
            --50000
            --Matches CSV - 50,000 rows x 8 columns (opened file to confirm)

         --ii. Make sure bulk insert read in text delimiter (") properly
            --Text delimiter used when commas found in a column like STORE_NAME.
            --If find commas in the column, then the text delimiter was read properly.
            SELECT      *
            FROM        raw_transaction_table 
            WHERE       STORE_NAME LIKE '%,%'
            --The text delimiter works.

         --iii. Check a numeric columns total column sum matches
             --This confirms no decimal places were lost.
             --Use CASE WHEN to convert blanks to 0s to allow column to sum. 
             WITH BLANKS_CONVERTED AS (
                 SELECT      FINAL_SALE
                            , CASE WHEN FINAL_SALE = '' THEN '0' --keep as string until ready to convert in next step
                                    ELSE FINAL_SALE END AS [Final_Sale_Numeric]
                FROM        raw_transaction_table 
                )
            --Temporarily cast FINAL_SALE and sum to confirm matches sum in EXCEL
            SELECT      SUM(CAST(Final_Sale_Numeric AS DECIMAL(30,10))) AS Total_FinalSale
            FROM        BLANKS_CONVERTED
            --171614.4000000000
            --MATCHES CSV: 171614.4 (opened file to confirm)
   

--2. Users Table

    --a.Create Table
    --set all columns to nvarchar(255) to avoid losing information by immediate conversion to non-string columns
    --DROP TABLE raw_user_table;
    CREATE TABLE raw_user_table (
          ID                nvarchar(255)
        , CREATED_DATE      nvarchar(255)
        , BIRTH_DATE        nvarchar(255)
        , [STATE]           nvarchar(255)
        , [LANGUAGE]        nvarchar(255)
        , GENDER            nvarchar(255)
        )

    --b. Bulk Insert
    --specify arguments that are consistent for a CSV file: 
    BULK INSERT raw_user_table 
    FROM '/var/opt/mssql/data/USER_TAKEHOME.csv'
    WITH (
        DATAFILETYPE = 'char'
        , FORMAT = 'CSV'
        , FIELDTERMINATOR = ','     --column delimitor
        , ROWTERMINATOR = '\n'      --row terminator
        , FIRSTROW = 2              --allows first row to be headers
       -- , CODEPAGE = '65001'      --Unable to specify bulk insert codepage in Linux, i.e. the way I am accessing SQL Server.
    )
    --(100000 rows affected)
    --NOTE: suspicous that exactly 100,000 rows -- is this all of the data?

    SELECT      TOP (100) *
    FROM        raw_user_table

    --c. Basic QC Check

        --i. Count of rows matches CSV
            SELECT      COUNT(1) AS [TotalRows]
            FROM        raw_user_table
            --Matches CSV - 100,000 rows x 6 columns (opened file to confirm)

        --ii. Make sure bulk insert read in text delimiter (") properly
            --Text delimiter used when commas found in a column. It does not appear to be any columns in this table.
            --Can confirm a different way by checking if the last column (GENDER) has a comma. Why? See below:
                --If a comma was found in any column, then the row would appear to have an additional column. However, there is no additional column to hold the information.
                --This would cause the final column to have multiple columns contained within it, and therefore a comma would appear in it.
            SELECT      *
            FROM        raw_user_table
            WHERE       GENDER LIKE '%,%'
            --The text delimiter works.

        --iii. Check a numeric columns total column sum matches
            --No numeric column.


--3. Products Table

    --a.Create Table
    --set all columns to nvarchar(255) to avoid losing information by immediate conversion to non-string columns
    --DROP TABLE raw_products_table;
    CREATE TABLE raw_products_table (
          CATEGORY_1        nvarchar(255)
        , CATEGORY_2        nvarchar(255)
        , CATEGORY_3        nvarchar(255)
        , CATEGORY_4        nvarchar(255)
        , [MANUFACTURER]    nvarchar(255)
        , BRAND             nvarchar(255)
        , BARCODE           nvarchar(255)
        )

    --b. Bulk Insert
    --specify arguments that are consistent for a CSV file: 
    BULK INSERT raw_products_table 
    FROM '/var/opt/mssql/data/PRODUCTS_TAKEHOME.csv'
    WITH (
        DATAFILETYPE = 'char'
        , FORMAT = 'CSV'
        , FIELDTERMINATOR = ','     --column delimitor
        , ROWTERMINATOR = '\n'      --row terminator
        , FIRSTROW = 2              --allows first row to be headers
       -- , CODEPAGE = '65001'      --Unable to specify bulk insert codepage in Linux, i.e. the way I am accessing SQL Server.
    )
    --(845552 rows affected)

    SELECT      TOP (100) *
    FROM        raw_products_table


    --c. Basic QC Check

        --i. Count of rows matches CSV
            SELECT      COUNT(1) AS [TotalRows]
            FROM        raw_products_table
            --Matches CSV - 845,552 rows x 7 columns (opened file to confirm)

        --ii. Make sure bulk insert read in text delimiter (") properly
            --Text delimiter used when commas found in a column like MANUFACTURER
            --If find commas in the column, then the text delimiter was read properly.
            SELECT      *
            FROM        raw_products_table
            WHERE       MANUFACTURER LIKE '%,%'
            --The text delimiter works.

        --iii. Check a numeric columns total column sum matches
            --This confirms no decimal places were lost.
            --Temporarily cast BARCODE and sum to confirm matches sum in EXCEL
            SELECT      SUM(CAST(BARCODE AS DECIMAL(30,10))) AS Total_FinalSale
            FROM        raw_products_table
            --506271821238590242.0000000000
            --MATCHES CSV: 506271821238590242 (opened file to confirm)


--END OF SCRIPT