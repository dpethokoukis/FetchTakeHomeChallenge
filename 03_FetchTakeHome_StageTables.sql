/***********************************************
Name:           Deirdre Pethokoukis
Script Name:    03_FetchTakeHome_StageTables
Database:       USE FetchTakeHome;
Purpose:        Stage 3 tables:
                    -Transactions
                    -Users
                    -Products
*************************************************/
---------------------
-- DATABASE SET UP --
---------------------

--1. Specify database to use for script
    USE FetchTakeHome;

--------------------------
-- STAGE TABLE         --
--------------------------

--1. Transaction Table

  --a. Create Table 
    --for nvarchar, will use standard 255 since the tables are not large enough to require more precision in size of column.
    --add a row_id to make identification of duplication/join blow ups more evident in future analysis.
        --because no way to uniquely identify a row, this ID field will can be different each time the table is created as row_number() function can't be consistent.
    -- DROP TABLE stage_transaction_table ;
    CREATE TABLE stage_transaction_table (
          RECEIPT_ID        nvarchar(255)
        , PURCHASE_DATE     datetime2
        , SCAN_DATE         datetime2
        , STORE_NAME        nvarchar(255)
        , USER_ID           nvarchar(255)
        , BARCODE           bigint
        , FINAL_QUANTITY    decimal(15,2)
        , FINAL_SALE        decimal(15,2)
        , row_id            int
        )

    --b. Insert into table
    --Will apply standard cleaning: triming of nvarchar fields to remove whitespace, turn blanks into nulls (for known NULL/blank columns)
    --Will adjust field values using knowledge learning during previous data quality checks
    INSERT INTO stage_transaction_table 
    SELECT      LTRIM(RTRIM(RECEIPT_ID))                         AS [RECEIPT_ID]
                , PURCHASE_DATE
                , SCAN_DATE
                , LTRIM(RTRIM(STORE_NAME))                      AS [STORE_NAME]
                , LTRIM(RTRIM(USER_ID))                         AS [USER_ID]
                , NULLIF(BARCODE,'')                               AS [BARCODE]
                , CASE WHEN FINAL_QUANTITY = 'zero' THEN '0'
                        ELSE FINAL_QUANTITY END                 AS [FINAL_QUANTITY]
                , NULLIF(FINAL_SALE,'')                    AS [FINAL_SALE]   
                , ROW_NUMBER() OVER(ORDER BY RECEIPT_ID, PURCHASE_DATE, SCAN_DATE, STORE_NAME, USER_ID, BARCODE, FINAL_QUANTITY, FINAL_SALE) AS [row_id]      
    FROM        raw_transaction_table
    --(50000 rows affected)
    
    SELECT      TOP (100) *
    FROM        stage_transaction_table 


--2. User Table
    --a.Create Table
    --for nvarchar, will use standard 255 since the tables are not large enough to require more precision in size of column.
    --add a row_id to make identification of duplication/join blow ups more evident in future analysis. Order by ID to make this row_id consistent each time create table.
    --DROP TABLE stage_user_table;
    CREATE TABLE stage_user_table (
          USER_ID                nvarchar(255)
        , CREATED_DATE      datetime2
        , BIRTH_DATE        datetime2
        , [STATE]           nvarchar(255)
        , [LANGUAGE]        nvarchar(255)
        , GENDER            nvarchar(255)
        , row_id            int
        )

    --b. Insert into table
    --Will apply standard cleaning: triming of nvarchar fields to remove whitespace, turn blanks into nulls (for known NULL/blank columns)
    --Will adjust field values using knowledge learning during previous data quality checks
    INSERT INTO stage_user_table 
    SELECT      LTRIM(RTRIM(ID))                         AS [USER_ID] --will rename USER_ID to make connection to USER_ID in transaction field more obvious
                , CREATED_DATE
                , NULLIF(BIRTH_DATE, '')                AS BIRTH_DATE
                , NULLIF([STATE], '')                   AS [STATE]
                , NULLIF([LANGUAGE], '')                AS [LANGUAGE]
                --Standardize gender as mentioned during investigation.
                , CASE WHEN [GENDER] = '' THEN NULL
                       WHEN [GENDER] = 'Prefer not to say' THEN 'prefer_not_to_say'                     
                       WHEN [GENDER] = 'Non-Binary' THEN 'non_binary'                    
                       WHEN [GENDER] = 'My gender isn''t listed' THEN 'not_listed'                    
                       WHEN [GENDER] = 'not_specified' THEN 'not_listed'                    
                    ELSE [GENDER] END                     AS [GENDER]
                , ROW_NUMBER() OVER(ORDER BY id) AS [row_id]      
    FROM        raw_user_table 
    --(100000 rows affected)
    
    SELECT      TOP (100) *
    FROM        stage_user_table
    

--3. Products Table
  --a.Create Table
    --add a row_id to make identification of duplication/join blow ups more evident in future analysis.
        --because no way to uniquely identify a row due to the NULLs, this ID field will can be different each time the table is created as row_number() function can't be consistent.
    --DROP TABLE stage_products_table;
    CREATE TABLE stage_products_table (
          CATEGORY_1        nvarchar(255)
        , CATEGORY_2        nvarchar(255)
        , CATEGORY_3        nvarchar(255)
        , CATEGORY_4        nvarchar(255)
        , [MANUFACTURER]    nvarchar(255)
        , BRAND             nvarchar(255)
        , BARCODE           bigint
        , row_id            int
        )

    --b. Insert into table 
    --Will apply standard cleaning: triming of nvarchar fields to remove whitespace, turn blanks into nulls (for known NULL/blank columns)
    --Will adjust field values using knowledge learning during previous data quality checks
    INSERT INTO stage_products_table 
    SELECT      LTRIM(RTRIM(
                    CASE WHEN CATEGORY_1 = '' THEN NULL
                         WHEN CATEGORY_1 = 'Needs Review' THEN NULL
                         ELSE CATEGORY_1 END))                                     AS [CATEGORY_1]
                ,  LTRIM(RTRIM(NULLIF(CATEGORY_2, '')))                            AS [CATEGORY_2]
                ,  LTRIM(RTRIM(NULLIF(CATEGORY_3, '')))                            AS [CATEGORY_3]
                ,  LTRIM(RTRIM(NULLIF(CATEGORY_4, '')))                            AS [CATEGORY_4]
                ,  LTRIM(RTRIM(NULLIF(MANUFACTURER, '')))                          AS [MANUFACTURER]
                ,  LTRIM(RTRIM(NULLIF(BRAND, '')))                                 AS [BRAND]
                ,  NULLIF(BARCODE,'')                                              AS [BARCODE]
                , ROW_NUMBER() OVER(ORDER BY barcode, CATEGORY_1, CATEGORY_2, CATEGORY_3, CATEGORY_4, MANUFACTURER, BRAND) AS [row_id]      
    FROM        raw_products_table 
    --(845552 rows affected)
    
    SELECT      TOP (100) *
    FROM        stage_products_table 


--END OF SCRIPT