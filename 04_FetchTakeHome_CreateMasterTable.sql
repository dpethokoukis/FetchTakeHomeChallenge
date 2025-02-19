/***********************************************
Name:           Deirdre Pethokoukis
Script Name:    04_FetchTakeHome_CreateMasterTable
Database:       USE FetchTakeHome;
Purpose:        Combine 3 Tables
                    -Transactions
                    -Users
                    -Purchases
*************************************************/
---------------------
-- DATABASE SET UP --
---------------------
--1. Specify database to use for script
    USE FetchTakeHome;

-------------------------------
-- TEST CONNECTING FIELDS    --
-------------------------------

--1. USER_ID in Transactions Table and Users Table

    --a. Which IDs appear in only one or both of the tables?
        --DROP TABLE IF EXISTS #user_id_match;
        --First, combine all IDs together to get a complete list
        WITH COMBINED AS (
            SELECT      DISTINCT USER_ID
            FROM        stage_transaction_table
            WHERE       USER_ID IS NOT NULL
            UNION 
            SELECT      DISTINCT USER_ID
            FROM        stage_user_table
            WHERE       USER_ID IS NOT NULL
        )
        --Join each of the tables onto the combined list to know which have a match or not
        , JOINS AS (
            SELECT      comb.USER_ID
                        , CASE WHEN [u].USER_ID IS NOT NULL THEN 'Yes'
                            WHEN [u].USER_ID IS NULL THEN 'No' 
                            ELSE NULL END AS [User_Table_Match]
                        , CASE WHEN [t].USER_ID IS NOT NULL THEN 'Yes'
                            WHEN [t].USER_ID IS NULL THEN 'No' 
                            ELSE NULL END AS [Transaction_Table_Match]
            FROM        COMBINED comb
            --Get unique list to avoid join blow up
            LEFT JOIN   (SELECT DISTINCT USER_ID FROM stage_user_table) u
                    ON [u].USER_ID = comb.USER_ID
            LEFT JOIN   (SELECT DISTINCT USER_ID FROM stage_transaction_table) t
                    ON [t].USER_ID = comb.USER_ID
        )
        --Put results into a temp table to review
        SELECT      *
        INTO        #user_id_match
        FROM        JOINS
        --(117603 rows affected)

        --i. View Results
        --Summarize matching
        SELECT      [User_Table_Match]          AS [Found in USERS Table]
                    , [Transaction_Table_Match] AS [Found in TRANSACTIONS Table]
                    , COUNT(1) AS [Total IDs]
        FROM        #user_id_match
        GROUP BY    [User_Table_Match]
                    , [Transaction_Table_Match]
        ORDER BY    [User_Table_Match]
                    , [Transaction_Table_Match]
        --There is barely matching. Points to incomplete data.

            --a. Example: Match
                SELECT      *
                FROM        #user_id_match
                WHERE       User_Table_Match = 'Yes'
                        AND Transaction_Table_Match = 'Yes'
                    
                    SELECT      *
                    FROM        stage_user_table
                    WHERE       USER_ID LIKE '5b441360be53340f289b0795'
                    
                    SELECT      *
                    FROM        stage_transaction_table
                    WHERE       USER_ID LIKE '5b441360be53340f289b0795'

            --b. Example: No match in User Table
                SELECT      *
                FROM        #user_id_match
                WHERE       User_Table_Match = 'No'
                        AND Transaction_Table_Match = 'Yes'
                    
                    SELECT      *
                    FROM        stage_user_table
                    WHERE       USER_ID LIKE '%574%732%'
                    
                    SELECT      *
                    FROM        stage_transaction_table
                    WHERE       USER_ID LIKE '%5748f001e4b03a732e4ecdc0%'

            --c. Example: No match in Transaction Table
                SELECT      *
                FROM        #user_id_match
                WHERE       User_Table_Match = 'Yes'
                        AND Transaction_Table_Match = 'No'
                    
                    SELECT      *
                    FROM        stage_user_table
                    WHERE       USER_ID LIKE '536178cfe4b012a86bd734f0'
                    
                    SELECT      *
                    FROM        stage_transaction_table
                    WHERE       USER_ID LIKE '%fe4b012a%'



--1. BARCODE in Transactions Table and Prodcuts Table

    --a. Which IDs appear in only one or both of the tables?
        --DROP TABLE IF EXISTS #barcode_match;
        --First, combine all barcodess together to get a complete list
        WITH COMBINED AS (
            SELECT      DISTINCT BARCODE
            FROM        stage_transaction_table
            WHERE       BARCODE IS NOT NULL
            UNION 
            SELECT      DISTINCT BARCODE
            FROM        stage_products_table
            WHERE       BARCODE IS NOT NULL
        )
        --Join each of the tables onto the combined list to know which have a match or not
        , JOINS AS (
            SELECT      comb.BARCODE
                        , CASE WHEN [p].BARCODE IS NOT NULL THEN 'Yes'
                            WHEN [p].BARCODE IS NULL THEN 'No' 
                            ELSE NULL END AS [Products_Table_Match]
                        , CASE WHEN [t].BARCODE IS NOT NULL THEN 'Yes'
                            WHEN [t].BARCODE IS NULL THEN 'No' 
                            ELSE NULL END AS [Transaction_Table_Match]
            FROM        COMBINED comb
            --Get unique list to avoid join blow up
            LEFT JOIN   (SELECT DISTINCT BARCODE FROM stage_products_table) p
                    ON [p].BARCODE = comb.BARCODE
            LEFT JOIN   (SELECT DISTINCT BARCODE FROM stage_transaction_table) t
                    ON [t].BARCODE = comb.BARCODE
        )
        --Put results into a temp table to review
        SELECT      *
        INTO        #barcode_match
        FROM        JOINS
        --(845807 rows affected)

        --i. View Results
        --Summarize matching
        SELECT      [Products_Table_Match]      AS [Found in PRODUCTS Table]
                    , [Transaction_Table_Match] AS [Found in TRANSACTIONS Table]
                    , COUNT(1) AS [Total IDs]
        FROM        #barcode_match
        GROUP BY    [Products_Table_Match]
                    , [Transaction_Table_Match]
        ORDER BY    [Products_Table_Match]
                    , [Transaction_Table_Match]
        --There is barely overlap. Points to incomplete data

            --a. Example: Match
                SELECT      *
                FROM        #barcode_match
                WHERE       [Products_Table_Match] = 'Yes'
                        AND Transaction_Table_Match = 'Yes'
                    
                    SELECT      *
                    FROM        stage_products_table
                    WHERE       BARCODE LIKE '22000135377'
                    
                    SELECT      *
                    FROM        stage_transaction_table
                    WHERE       BARCODE LIKE '22000135377'

            --b. Example: No match in Products Table
                SELECT      *
                FROM        #barcode_match
                WHERE       [Products_Table_Match] = 'No'
                        AND Transaction_Table_Match = 'Yes'
                    
                    SELECT      *
                    FROM        stage_products_table
                    WHERE       BARCODE LIKE '%962826%'
                    ORDER BY    BARCODE
                    
                    SELECT      *
                    FROM        stage_transaction_table
                    WHERE       BARCODE LIKE '%70896282675%'

            --c.Example:  No match in Transaction Table
                SELECT      *
                FROM        #barcode_match
                WHERE       [Products_Table_Match] = 'Yes'
                        AND Transaction_Table_Match = 'No'
                    
                    SELECT      *
                    FROM        stage_products_table
                    WHERE       BARCODE LIKE '%22000124807%'
                    
                    SELECT      *
                    FROM        stage_transaction_table
                    WHERE       BARCODE LIKE '%12480%'
                    ORDER BY    BARCODE


--------------------------
-- CREATE MASTER TABLE  --
--------------------------

--The transaction table is the main table. We join the other data to get more detail about the transactions.

--1. Create Table

    --a. Create Table
    --add a row_id to make identification of duplication/join blow ups more evident in future analysis.
    --DROP TABLE IF EXISTS stage_master_table;
    CREATE TABLE stage_master_table (
          RECEIPT_ID        nvarchar(255)
        , PURCHASE_DATE     datetime2
        , SCAN_DATE         datetime2
        , STORE_NAME        nvarchar(255)
        , USER_ID           nvarchar(255)
        , BARCODE           bigint
        , FINAL_QUANTITY    decimal(15,2)
        , FINAL_SALE        decimal(15,2)
        , DUPE_CHECK        int

        --Users Table
        , USERID_CREATED_DATE      datetime2
        , USERID_BIRTH_DATE        datetime2
        , [USERID_STATE]           nvarchar(255)
        , [USERID_LANGUAGE]        nvarchar(255)
        , USERID_GENDER            nvarchar(255)

        --Products Table
        , BARCODE_CATEGORY_1        nvarchar(255)
        , BARCODE_CATEGORY_2        nvarchar(255)
        , BARCODE_CATEGORY_3        nvarchar(255)
        , BARCODE_CATEGORY_4        nvarchar(255)
        , [BARCODE_MANUFACTURER]    nvarchar(255)
        , BARCODE_BRAND             nvarchar(255)
       , row_id            int
       )


    --b. Insert Table
    INSERT INTO stage_master_table 
                --Transaction Table
    SELECT      [t].RECEIPT_ID
                , [t].PURCHASE_DATE
                , [t].SCAN_DATE
                , [t].STORE_NAME
                , [t].USER_ID
                , [t].BARCODE
                , [t].FINAL_QUANTITY
                , [t].FINAL_SALE
                , [t].[row_id] AS [DUPE_CHECK]
                --User Table
                , [u].CREATED_DATE  AS [USERID_CREATED_DATE]
                , [u].BIRTH_DATE    AS [USERID_BIRTH_DATE]
                , [u].[STATE]       AS [USERID_STATE]
                , [u].[LANGUAGE]    AS [USERID_LANGUAGE]
                , [u].GENDER        AS [USERID_GENDER]
                --Products Table
                , [p].CATEGORY_1    AS BARCODE_CATEGORY_1       
                , [p].CATEGORY_2    AS BARCODE_CATEGORY_2        
                , [p].CATEGORY_3    AS BARCODE_CATEGORY_3       
                , [p].CATEGORY_4    AS BARCODE_CATEGORY_4        
                , [p].MANUFACTURER  AS [BARCODE_MANUFACTURER]    
                , [p].BRAND         AS BARCODE_BRAND     
                , ROW_NUMBER() OVER(ORDER BY [t].RECEIPT_ID, [t].PURCHASE_DATE, [t].SCAN_DATE, [t].STORE_NAME, [t].USER_ID, [t].BARCODE, [t].FINAL_QUANTITY, [t].FINAL_SALE) AS [row_id]                   
    FROM        stage_transaction_table t
    LEFT JOIN   (SELECT * FROM stage_user_table WHERE USER_ID IS NOT NULL) u
                ON [t].USER_ID = [u].USER_ID
    LEFT JOIN   (SELECT * FROM stage_products_table WHERE barcode IS NOT NULL) p
                ON [t].BARCODE = [p].BARCODE
    --(50024 rows affected)


    --c. Check Duplication 
        SELECT      DUPE_CHECK
                    , COUNT(1) AS [TotalRows]
        FROM        stage_master_table
        GROUP BY    DUPE_CHECK
        HAVING      COUNT(1) > 1
        ORDER BY    TotalRows DESC
        --Only 24, noted duplicated issue already.
        --Only ever 2 which tracks with the most number of rows a barcode can have in product table.


--END OF SCRIPT