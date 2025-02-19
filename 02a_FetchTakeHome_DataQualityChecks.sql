/***********************************************
Name:           Deirdre Pethokoukis
Script Name:    02a_FetchTakeHome_DataQualityChecks
Database:       USE FetchTakeHome;
Purpose:        Perform Data Quality Checks
                to prepare for staging of 3 tables:
                    -Transactions
                    -Users
                    -Products
*************************************************/
---------------------
-- DATABASE SET UP --
---------------------

--1. Specify database to use for script
    USE FetchTakeHome;


------------------------------------
-- INVESTIGATE TABLE FOR STAGING  --
-- AND DATA QUALITY               --
------------------------------------

--1. Transaction Table
    SELECT TOP (100) *
    FROM raw_transaction_table

    /* GENERAL QUESTION: WHAT DO FIELDS MEAN?*/

    --a. Check NULLs
        --For each field, sum the total number of times the field is NULL or blank.
        --If NULL or Blank, add 1. Otherwise, add 0.
       SELECT SUM(CASE WHEN RECEIPT_ID      IS NULL OR RECEIPT_ID       = '' THEN 1 ELSE 0 END)    AS [RECEIPT_ID_NULLCount]
            , SUM(CASE WHEN PURCHASE_DATE   IS NULL OR PURCHASE_DATE    = '' THEN 1 ELSE 0 END)    AS [PURCHASE_DATE_NULLCount]
            , SUM(CASE WHEN SCAN_DATE       IS NULL OR SCAN_DATE        = '' THEN 1 ELSE 0 END)    AS [SCAN_DATE_NULLCount]
            , SUM(CASE WHEN STORE_NAME      IS NULL OR STORE_NAME       = '' THEN 1 ELSE 0 END)    AS [STORE_NAME_NULLCount]
            , SUM(CASE WHEN USER_ID         IS NULL OR USER_ID          = '' THEN 1 ELSE 0 END)    AS [USER_ID_NULLCount]
            , SUM(CASE WHEN BARCODE         IS NULL OR BARCODE          = '' THEN 1 ELSE 0 END)    AS [BARCODE_NULLCount]
            , SUM(CASE WHEN FINAL_QUANTITY  IS NULL OR FINAL_QUANTITY   = '' THEN 1 ELSE 0 END)    AS [FINAL_QUANTITY_NULLCount]
            , SUM(CASE WHEN FINAL_SALE      IS NULL OR FINAL_SALE       = '' THEN 1 ELSE 0 END)    AS [FINAL_SALE_NULLCount]
        FROM  raw_transaction_table
        --BARCODE has 5762 / 50000 rows NULL/blank, FINAL_SALE has 12500 / 50000 rows NULL/blank
        --Will check if any pattern to when these are NULL/blank in each field's individual investigation below.

    --b. Individual Fields Investigation

        --i. RECEIPT_ID - nvarchar
            --a. Confirm consistent Length
            --would think a ID field would have consistent length 
            SELECT      LEN(RECEIPT_ID) AS [Field_Length]
                        , COUNT(1) AS [TotalRows]
            FROM        raw_transaction_table
            GROUP BY    LEN(RECEIPT_ID)
            ORDER BY    TotalRows DESC
            --has consistent length of 36

            --b. Duplication of an ID?
                SELECT      RECEIPT_ID
                            , COUNT(1) AS [TotalRows]
                FROM        raw_transaction_table
                GROUP BY    RECEIPT_ID
                ORDER BY    TotalRows DESC
                --24,440 unique IDs

                --i. Example of ID with multiple rows: bedac253-2256-461b-96af-267748e6cecf
                    SELECT      *
                    FROM        raw_transaction_table
                    WHERE       RECEIPT_ID = 'bedac253-2256-461b-96af-267748e6cecf'
                    --Has the same PURCHASE_DATE, SCAN_DATE, and BARCODE
                        --This makes sense for PURCHASE/SCAN_DATE as all items on a receipt will have the same date from the receipt
                        --and are scanned at the exact same time into the app.
                        --It may be interesting to investigate why the same BARCODE appears multiple times because there is a "FINAL_QUANTITY" field
                        --that would in theory increment if multiple units. Will investigate below.

                --ii. How many IDs have multiple rows (of the 24,440)?
                    SELECT      RECEIPT_ID
                                , COUNT(1) AS [TotalRows]
                    FROM        raw_transaction_table
                    GROUP BY    RECEIPT_ID
                    HAVING      COUNT(1) > 1
                    ORDER BY    TotalRows DESC
                    --all have multiple rows


        --ii. PURCHASE_DATE - datetime2
            --a. Confirm any conversion errors using datetime2
                SELECT PURCHASE_DATE
                        , TRY_CAST(PURCHASE_DATE AS DATETIME2) AS [CAST_PURCHASE_DATE]
                FROM    raw_transaction_table
                --WHERE   TRY_CAST(PURCHASE_DATE AS DATETIME2) IS NULL
                --No information is lost using this datatype.
                --TRY_CAST is never NULL, therefore conversion is successful.

            --b. Ever any time information?
                --If the hour is ever different from 0, then we know there is time information.
                SELECT      DATEPART(hh, CAST(PURCHASE_DATE AS DATETIME2)) AS [HourofTime]
                            , COUNT(1) AS [TotalRows]
                FROM    raw_transaction_table
                GROUP BY    DATEPART(hh, CAST(PURCHASE_DATE AS DATETIME2)) 
                ORDER BY    DATEPART(hh, CAST(PURCHASE_DATE AS DATETIME2)) 
                --Never any time data. could save space by saving as a date, but will use that datetype provided in instructions (datetime2).

            --c. Confirm only one PURCHASE_DATE for each RECEIPT_ID?
                --Would expect only 1 distinct PURCHASE_DATE per RECEIPT_ID.
                SELECT  RECEIPT_ID
                        , COUNT(DISTINCT PURCHASE_DATE) AS [UniquePurchaseDates]
                FROM    raw_transaction_table
                GROUP BY RECEIPT_ID
                HAVING COUNT(DISTINCT PURCHASE_DATE) <> 1
                --Never more than 1. Therefore, always one purchase date for all 24,440 unique RECEIPT_IDs.
          
            --d. Any outliers?
                --Make conversion to date (use date instead of date time because want to investigates dates, not times)        
                WITH DECIMAL_CONVERSION AS (
                    SELECT CAST(PURCHASE_DATE AS DATETIME2)AS [CAST_PURCHASE_DATE]
                    FROM    raw_transaction_table
                )
                --See the spread
                SELECT      [CAST_PURCHASE_DATE]
                            , COUNT(1)
                FROM        DECIMAL_CONVERSION
                GROUP BY    [CAST_PURCHASE_DATE]
                ORDER BY    [CAST_PURCHASE_DATE]
                --6/12/2024 - 9/8/2024
                --No dates with way more data or falling way outside the date range.
                --See Python Graph "01_Number of Records per Purchase Date"

            --e. Any days missing between the min and max date?
                --Going to see if any gaps between the previous date and current date of more than 1
                --Make conversion to date (use date instead of date time because want to investigates dates, not times)        
                WITH DATE_CONVERSION AS (
                        SELECT CAST(PURCHASE_DATE AS DATE)AS [CAST_PURCHASE_DATE]
                        FROM    raw_transaction_table
                    )
                --Grab previous date for each distinct date
                , LAG_DATE AS (
                        SELECT      [CAST_PURCHASE_DATE]
                                    , LAG([CAST_PURCHASE_DATE],1, NULL) OVER(ORDER BY [CAST_PURCHASE_DATE]) AS [Previous_PURCHASE_DATE]
                        FROM        DATE_CONVERSION 
                        GROUP BY    [CAST_PURCHASE_DATE]
                )
                --Take difference between the two dates
                SELECT      *
                            , DATEDIFF(d, [Previous_PURCHASE_DATE],[CAST_PURCHASE_DATE]) AS [Days_Difference]
                FROM        LAG_DATE
                ORDER BY    [CAST_PURCHASE_DATE]
                --The days difference is never > 1, no days missing in dataset.


        --iii. SCAN_DATE - datetime2
            --NOTE: Why does the name say only date but also has times? Would be better to all it ScanDateTime

            --a. Confirm any conversion errors using datetime2
                SELECT SCAN_DATE
                        , TRY_CAST(SCAN_DATE AS DATETIME2) AS [CAST_SCAN_DATE]
                FROM    raw_transaction_table
                WHERE   TRY_CAST(SCAN_DATE AS DATETIME2) IS NULL
                --No information is lost using this datatype.
                --TRY_CAST is never NULL, therefore conversion is successful.

            --b. Confirm only one SCAN_DATE for each RECEIPT_ID?
                SELECT  RECEIPT_ID
                        , COUNT(DISTINCT SCAN_DATE) AS [UniqueScanDates]
                        , COUNT(1) AS [TotalRows]
                FROM    raw_transaction_table
                GROUP BY RECEIPT_ID
                HAVING COUNT(DISTINCT SCAN_DATE) <> 1
                --Never more than 1. Therefore, always one SCAN_DATE for all 24,440 unique RECEIPT_IDs.

            --c. Any outliers?
                --Make conversion to date (use date instead of date time because want to investigates dates, not times)        
                WITH DECIMAL_CONVERSION AS (
                    SELECT CAST(SCAN_DATE AS DATETIME2)AS [CAST_SCAN_DATE]
                    FROM    raw_transaction_table
                )
                --See the spread
                SELECT      [CAST_SCAN_DATE]
                            , COUNT(1)
                FROM        DECIMAL_CONVERSION
                GROUP BY    [CAST_SCAN_DATE]
                ORDER BY    [CAST_SCAN_DATE]
                --6/12/2024 - 9/8/2024
                --No dates with way more data or falling way outside the date range.

            --d. Is SCAN_DATE ever different from PURCHASE_DATE?
                --i. Is there a difference?     
                    WITH DATE_CONVERSION AS (
                            SELECT CAST(PURCHASE_DATE AS DATE)AS [CAST_PURCHASE_DATE]
                                , CAST(SCAN_DATE AS DATE) AS [CAST_SCAN_DATE]
                            FROM    raw_transaction_table
                        )
                    SELECT      [CAST_PURCHASE_DATE]
                                , [CAST_SCAN_DATE]
                    FROM        DATE_CONVERSION 
                    WHERE       [CAST_PURCHASE_DATE] <> [CAST_SCAN_DATE]
                    --Yes, it is often different, which can be expected.

                --ii. What is the difference between the two if different?  
                    WITH DATE_CONVERSION AS (
                            SELECT CAST(PURCHASE_DATE AS DATE)AS [CAST_PURCHASE_DATE]
                                , CAST(SCAN_DATE AS DATE) AS [CAST_SCAN_DATE]
                            FROM    raw_transaction_table
                        )
                    --Calculate the difference between SCAN_DATE AND PURCHASE_DATE
                    , DAYS_DIFFERENT AS (
                        SELECT      [CAST_PURCHASE_DATE]
                                    , [CAST_SCAN_DATE]
                                    , DATEDIFF(d, [CAST_PURCHASE_DATE],[CAST_SCAN_DATE]) AS [Days_Difference]
                        FROM        DATE_CONVERSION 
                        WHERE       [CAST_PURCHASE_DATE] <> [CAST_SCAN_DATE]
                    )
                    --How often are the difference in days occuring
                    SELECT      [Days_Difference]
                                , COUNT(1) AS [TotalRows]
                    FROM        DAYS_DIFFERENT
                    GROUP BY    [Days_Difference]
                    ORDER BY    [Days_Difference] DESC
                    --It can be up to a month after the purchase date that it is scanned.
                    --A user has up until 14 days to scan. So these users are late!


        --iv. STORE_NAME - nvarchar
            --a. Confirm STORE_NAME is standardized, i.e. does the same store appear under multiple spellings, variations, etc.
                SELECT      DISTINCT STORE_NAME AS [StoreName]
                FROM        raw_transaction_table
                ORDER BY    STORE_NAME
                --Quick check shows standardization. Could spend more time here if needed.

            --b. Investigate ones that have non-alphanumeric characters
                SELECT      DISTINCT STORE_NAME AS [StoreName]
                FROM        raw_transaction_table
                WHERE       PATINDEX('%[^a-zA-Z0-9 ]%',STORE_NAME) >= 1 --searching for any non-alphnumeric columns
                ORDER BY    STORE_NAME
                --There are some potential clean-up to perform in names
                --For example: "IL''S WHIOLESALE CLUB" (is it supposed to have double apostrophes and mispelled wholesale) OR "K ENT 'S MARKE" (weird spaces and potential missing letters)
                        SELECT      *
                        FROM        raw_transaction_table
                        WHERE       STORE_NAME = 'K ENT ''S MARKE'
                                    OR STORE_NAME LIKE '%WHIO%'

            --c. Investigate foreign characters         
                --Look for non-ASCII characters
                SELECT      DISTINCT STORE_NAME AS [StoreName]
                FROM        raw_transaction_table
                WHERE       STORE_NAME COLLATE Latin1_General_BIN LIKE '%[^ -~]%';                     
                --FRESCO Y M├üS
                --LAΓÇÖBONNEΓÇÖS MARKETS
                --Do not appear like this in CSV. Would need to use UTF codepage (unable to specify bulk insert codepage in Linux as I am using to access SQL server on personal computer)


        --v. USER_ID - nvarchar
            --a. Consistent Length
                --would think a ID field would have consistent length 
                SELECT      LEN(USER_ID) AS [Field_Length]
                            , COUNT(1) AS [TotalRows]
                FROM        raw_transaction_table
                GROUP BY    LEN(USER_ID)
                ORDER BY    TotalRows DESC
                --has consistent length of 24

            --b. Duplication of an ID?
                SELECT      USER_ID
                            , COUNT(1) AS [TotalRows]
                            , COUNT(DISTINCT RECEIPT_ID) AS [TotalReceipts]
                FROM        raw_transaction_table
                GROUP BY    USER_ID
                ORDER BY    TotalRows DESC
                --17,694
                --Would expect given a user (presumed customer) would purchase multiple items at once
                --Would expect a user to potential scan multiple receipts.               

            --c. Is there Multiple USER_IDs within a RECEIPT_ID?
                --Want to confirm a RECEIPT_ID is unique to a user as this would be expected.
                SELECT  RECEIPT_ID
                        , COUNT(DISTINCT USER_ID) AS [UniqueScanDates]  
                FROM    raw_transaction_table
                GROUP BY RECEIPT_ID
                HAVING COUNT(DISTINCT USER_ID) <> 1
                --0, as expected


        --vi. Barcode - Integer
            --a. Confirm any conversion errors using bigint for rows with a Barcode
                SELECT BARCODE
                        , TRY_CAST(BARCODE AS BIGINT) AS [CAST_BARCODE]
                FROM    raw_transaction_table
                WHERE   TRY_CAST(BARCODE AS BIGINT) IS NULL
                    --ignore rows that don't have a barcode
                    AND BARCODE IS NOT NULL
                    AND BARCODE <> ''
                --No information is lost using this datatype.
                --TRY_CAST is never NULL, therefore conversion is successful.

            --b. Length Comparison
                SELECT      LEN(BARCODE) AS [Field_Length]
                            , COUNT(1) AS [TotalRows]
                FROM        raw_transaction_table
                GROUP BY    LEN(BARCODE)
                ORDER BY    TotalRows DESC
                --length can vary. Why?

                --i. Example of length of 13
                    SELECT      *
                    FROM        raw_transaction_table
                    WHERE       LEN(BARCODE) = 13

                --ii. Can it vary within a store?
                    --Does a store ever have more than 1 barcode length?
                    SELECT      STORE_NAME
                                , COUNT(DISTINCT LEN(BARCODE)) AS [UniqueField_Length]
                    FROM        raw_transaction_table
                    GROUP BY    STORE_NAME
                    HAVING      COUNT(DISTINCT LEN(BARCODE)) > 1
                    --Yes, some stores do.

            --c. BARCODE duplication within RECEIPT_ID investigation
                --i. Is the BARCODE always the same within the recipet id?
                    --USE ISNULL(NULLIF(BARCODE,''),0)) to convert all blanks to NULLs, and then all NULLs (and now blanks) to 0s so they are counted in the COUNT DISTINCT 
                    SELECT  RECEIPT_ID
                            , COUNT(DISTINCT ISNULL(NULLIF(BARCODE,''),0)) AS [UniqueBARCODES]
                            , COUNT(ISNULL(NULLIF(BARCODE,''),0)) AS [TotalRows]
                    FROM    raw_transaction_table
                    GROUP BY RECEIPT_ID
                    HAVING COUNT(DISTINCT ISNULL(NULLIF(BARCODE,''),0)) <> COUNT(1)
                    --The number of rows is never the same as unique BARCODES (all 24,440 RECEIPTs have this charactersitic)
                    --Why does the same BARCODE appears multiple times because there is a "FINAL_QUANTITY" field

                --ii. Is there ever more than one BARCODE within a RECEIPT_ID
                    SELECT  RECEIPT_ID
                            , COUNT(DISTINCT BARCODE) AS [UniqueBarccodes]
                            , COUNT(1) AS [TotalRows]
                    FROM    raw_transaction_table
                    GROUP BY RECEIPT_ID
                    HAVING COUNT(DISTINCT BARCODE) NOT IN (0,1)
                    --can have multiple BARCODES but not common - 329 instances

            --d. Investigation of NULLs and Blanks

                --i. What is the quantity for these rows?
                    SELECT      FINAL_QUANTITY
                                , COUNT(1) AS [TotalRows]
                    FROM        raw_transaction_table
                    WHERE       BARCODE IS NULL  
                            OR BARCODE = ''
                    GROUP BY    FINAL_QUANTITY
                    ORDER BY    FINAL_QUANTITY
                    --Quantity varies, not just zero.

                --ii. What is final_sale for these rows?
                    SELECT      FINAL_SALE
                                , COUNT(1) AS [TotalRows]
                    FROM        raw_transaction_table
                    WHERE       BARCODE IS NULL  
                            OR BARCODE = ''
                    GROUP BY    FINAL_SALE
                    ORDER BY    FINAL_SALE
                    --Can be blank but some have values.

                --iii. How many attributed to each store?
                    SELECT      STORE_NAME
                                , COUNT(1) AS [TotalRows]
                    FROM        raw_transaction_table
                    WHERE       BARCODE IS NULL  
                            OR BARCODE = ''
                    GROUP BY    STORE_NAME
                    ORDER BY    [TotalRows] DESC
                    --Aldi and CSV have the most.

                --iv. Is it missing only during a certain time period?
                    SELECT      MIN (PURCHASE_DATE) AS [MIN_PURCHASE_DATE]
                                , MAX(PURCHASE_DATE) AS [MAX_PURCHASE_DATE]
                                , MIN (SCAN_DATE) AS [MIN_SCAN_DATE]
                                , MAX(SCAN_DATE) AS [MAX_SCAN_DATE]
                    FROM        raw_transaction_table
                    WHERE       FINAL_SALE IS NULL  
                            OR FINAL_SALE = ''
                    --takes place over the entire range of purchases/scan dates.


        --vii. FINAL_QUANTITY - Numeric
            --a. Confirm any conversion errors using decimal(15,2)
                --i.Check if data after the decimal
                    SELECT      *
                    FROM        raw_transaction_table
                    WHERE       RIGHT(FINAL_QUANTITY,2) <> '00' --if not equal to 00, then has decimal values.
                            AND FINAL_QUANTITY <> 'zero'
                    --yes

                --ii. Check if more than 2 decimal places
                --If always 2 decimal places, the 3rd value from the right should be a period.
                    SELECT      *
                    FROM        raw_transaction_table
                    WHERE       LEFT(RIGHT(FINAL_QUANTITY,3),1) <> '.' --if not equal to 00, then has decimal values.
                            AND FINAL_QUANTITY <> 'zero'
                    --always 2 decimal places

                --iii.Use decimal instead of integer to make avoid losing data as there are quantities with data after the decimal.
                    SELECT FINAL_QUANTITY
                            , TRY_CAST(FINAL_QUANTITY AS decimal(15,2)) AS [CAST_FINAL_QUANTITY]
                    FROM    raw_transaction_table
                    WHERE   TRY_CAST(FINAL_QUANTITY AS decimal(15,2))IS NULL
                        --Will use case when when stage table to convert from zero to 0
                        AND FINAL_QUANTITY  <> 'zero' --
                    --No information is lost using this datatype.
                    --TRY_CAST is never NULL, therefore conversion is successful.

            --b. Any outliers?
                --Will ignore the zeroes for now because not outliers
                --Make conversion to decimal first          
                WITH DECIMAL_CONVERSION AS (
                    SELECT FINAL_QUANTITY
                            , CAST(FINAL_QUANTITY AS decimal(15,2)) AS [CAST_FINAL_QUANTITY]
                    FROM    raw_transaction_table
                    WHERE   FINAL_QUANTITY  <> 'zero' 
                )
                --See the spread
                SELECT      [CAST_FINAL_QUANTITY]
                            , COUNT(1)
                FROM        DECIMAL_CONVERSION
                GROUP BY    [CAST_FINAL_QUANTITY]
                ORDER BY    [CAST_FINAL_QUANTITY]
                --The only outlier appears to be 276. However, no product info to understand if this quantity makes sense.

                --i. Example of 276
                    SELECT      *
                    FROM        raw_transaction_table
                    WHERE       TRY_CAST(FINAL_QUANTITY AS decimal(15,2)) = 276.00
                    --Would need more context to understand if this is valid or not

                    SELECT      *
                    FROM        raw_products_table
                    WHERE       BARCODE LIKE '%48001353664%'
                    --no information for barcode


        --viii. FINAL_SALE - Numeric
            --a. Confirm any conversion errors using decimal(15,2)
                --i.Check if data after the decimal
                    SELECT      *
                    FROM        raw_transaction_table
                    WHERE       RIGHT(FINAL_SALE,2) <> '00' --if not equal to 00, then has decimal values.
                            AND FINAL_SALE <> ''
                    --yes

                --ii. Check if more than 2 decimal places
                    --If always 2 decimal places, the 3rd value from the right should be a period.
                    SELECT      *
                    FROM        raw_transaction_table
                    WHERE       LEFT(RIGHT(FINAL_SALE,3),1) <> '.' --if not equal to 00, then has decimal values.
                            AND FINAL_SALE <> ''
                    --always 2 decimal places

                --iii.Use decimal instead of integer to make avoid losing data as there are quantities with data after the decimal.
                    SELECT FINAL_SALE
                            , TRY_CAST(FINAL_SALE AS decimal(15,2)) AS [CAST_FINAL_SALE]
                    FROM    raw_transaction_table
                    WHERE   TRY_CAST(FINAL_SALE AS decimal(15,2))IS NULL
                        --Will use case when when stage table to convert from '' to NULL
                        AND FINAL_SALE  <> '' 
                    --No information is lost using this datatype.
                    --TRY_CAST is never NULL, therefore conversion is successful.

            --b. Any outliers?
                --Will ignore the blanks for now because not outliers
                --Make conversion to decimal first          
                WITH DECIMAL_CONVERSION AS (
                    SELECT FINAL_SALE
                            , CAST(FINAL_SALE AS decimal(15,2)) AS [CAST_FINAL_SALE]
                    FROM    raw_transaction_table
                    WHERE   FINAL_SALE  <> '' 
                )
                --See the spread
                SELECT      [CAST_FINAL_SALE]
                            , COUNT(1)
                FROM        DECIMAL_CONVERSION
                GROUP BY    [CAST_FINAL_SALE]
                ORDER BY    [CAST_FINAL_SALE]
                --The only outlier appears to be a few over $200
                --See the Python Graph “02_Final Sale Outliers”.

                --i. Example of over $200
                    SELECT      *
                    FROM        raw_transaction_table
                    WHERE       TRY_CAST(FINAL_SALE AS decimal(15,2)) > 200
                    --Would need more context to understand if this is valid or not, i.e. what the product is.


            --c. FINAL_QUANTITY vs. FINAL_SALE

                --i. How often is FINAL_QUANTITY zero and FINAL_SALE > 0?
                    --If final sale is the final amount paid, would expect there to be a quantity
                    SELECT       *
                    FROM        raw_transaction_table
                    WHERE   (    TRY_CAST(FINAL_QUANTITY AS decimal(15,2)) = 0
                            OR FINAL_QUANTITY = 'zero' )
                            AND TRY_CAST(FINAL_SALE AS decimal(15,2)) > 0
                    --12,341 / 50,000 (25%), this is a high proportion

                --ii. How often is FINAL_QUANTITY > 0 and FINAL_SALE is blank/zero/NULL
                    --If final sale is the final amount paid, would expect there to be a quantity
                    SELECT       *
                    FROM        raw_transaction_table
                    WHERE      TRY_CAST(FINAL_QUANTITY AS decimal(15,2)) > 0
                            AND (TRY_CAST(FINAL_SALE AS decimal(15,2)) = 0
                                OR FINAL_SALE IS NULL
                                OR FINAL_SALE = ''
                            )
                    --12,821 / 50,000 (26%), this is a high proportion

            --d. Investigation of NULLs and Blanks
            SELECT      *
            FROM        raw_transaction_table
            WHERE       FINAL_SALE IS NULL  
                    OR FINAL_SALE = ''
            --12500 rows, why?

                --i. Is it missing only during a certain time period?
                    SELECT      MIN (PURCHASE_DATE) AS [MIN_PURCHASE_DATE]
                                , MAX(PURCHASE_DATE) AS [MAX_PURCHASE_DATE]
                                , MIN (SCAN_DATE) AS [MIN_SCAN_DATE]
                                , MAX(SCAN_DATE) AS [MAX_SCAN_DATE]
                    FROM        raw_transaction_table
                    WHERE       FINAL_SALE IS NULL  
                            OR FINAL_SALE = ''
                    --takes place over the entire range of purchase/scan dates.


    --c. What represents a unique record in the transaction table?
        --If we use all fields, is there multiple records for any combination of field values?
        SELECT      RECEIPT_ID
                    , PURCHASE_DATE
                    , SCAN_DATE
                    , STORE_NAME
                    , USER_ID
                    , BARCODE
                    , FINAL_QUANTITY
                    , FINAL_SALE
                    , COUNT(1) AS [TotalRows]
        FROM        raw_transaction_table
        GROUP BY    RECEIPT_ID
                    , PURCHASE_DATE
                    , SCAN_DATE
                    , STORE_NAME
                    , USER_ID
                    , BARCODE
                    , FINAL_QUANTITY
                    , FINAL_SALE
        HAVING      COUNT(1) > 1 
        --149
        --There is no unique identifier. Either missing an additional field, there is duplication, 
            --or the same products was purchased multiple times on a receipt and appeared as two lines (instead of incrementing FINAL_QUANTITY).


--2. User Table
    SELECT TOP (100) *
    FROM raw_user_table

    /* GENERAL QUESTION: WHAT DO FIELDS MEAN?*/

    --a. Check NULLs
        --For each field, sum the total number of times the field is NULL or blank.
       SELECT SUM(CASE WHEN ID              IS NULL OR ID              = '' THEN 1 ELSE 0 END)    AS [ID_NULLCount]
            , SUM(CASE WHEN CREATED_DATE    IS NULL OR CREATED_DATE    = '' THEN 1 ELSE 0 END)    AS [CREATED_DATE_NULLCount]
            , SUM(CASE WHEN BIRTH_DATE      IS NULL OR BIRTH_DATE      = '' THEN 1 ELSE 0 END)    AS [BIRTH_DATE_NULLCount]
            , SUM(CASE WHEN [STATE]         IS NULL OR [STATE]         = '' THEN 1 ELSE 0 END)    AS [STATE_NULLCount]
            , SUM(CASE WHEN [LANGUAGE]      IS NULL OR [LANGUAGE]      = '' THEN 1 ELSE 0 END)    AS [LANGUAGE_NULLCount]
            , SUM(CASE WHEN GENDER          IS NULL OR GENDER          = '' THEN 1 ELSE 0 END)    AS [GENDER_NULLCount]
        FROM  raw_user_table
        --BIRTH_DATE has 3675/ 100000 (4%) rows NULL/blank, STATE has 4812/ 100000 (5%) rows NULL/blank
            --LANGUAGE has 30508 / 100000 (31%) rows NULL/blank , GENDER has 5892 (6%) rows NULL/blank
        --Will check if any pattern to when these are NULL/blank

    --b. Individual Fields Investigation

       --i. ID - nvarchar
            --a. Confirm consistent Length
                --would think a ID field would have consistent length 
                SELECT      LEN(ID) AS [Field_Length]
                            , COUNT(1) AS [TotalRows]
                FROM        raw_user_table
                GROUP BY    LEN(ID)
                ORDER BY    TotalRows DESC
                --has consistent length of 24
            
            --b. Duplication of an ID?
                SELECT      ID
                            , COUNT(1) AS [TotalRows]
                FROM        raw_user_table
                GROUP BY    ID
                ORDER BY    TotalRows DESC
                --Only 1 record per ID - this is the unique identifier.


        --ii. CREATED_DATE - datetime2
            --a. Confirm any conversion errors using datetime2
                SELECT CREATED_DATE
                        , TRY_CAST(CREATED_DATE AS DATETIME2) AS [CAST_CREATED_DATE]
                FROM    raw_user_table
                --WHERE   TRY_CAST(CREATED_DATE AS DATETIME2) IS NULL
                --No information is lost using this datatype.
                --TRY_CAST is never NULL, therefore conversion is successful.
          
            --b. Any outliers?
                --Make conversion to date (use date instead of date time because want to investigates dates, not times)        
                WITH DECIMAL_CONVERSION AS (
                    SELECT CAST(CREATED_DATE AS DATETIME2)AS [CAST_CREATED_DATE]
                    FROM    raw_user_table
                )
                --See the spread
                SELECT      [CAST_CREATED_DATE]
                            , COUNT(1)
                FROM        DECIMAL_CONVERSION
                GROUP BY    [CAST_CREATED_DATE]
                ORDER BY    [CAST_CREATED_DATE]
                --4/18/2014 - 9/11/2024
                --No dates with way more data or falling way outside the date range

            --c. Any large gaps between the min and max date?
                --Going to see if any gaps between the previous date and current date of more than 1
                --Make conversion to date (use date instead of date time because want to investigates dates, not times)        
                WITH DATE_CONVERSION AS (
                        SELECT CAST(CREATED_DATE AS DATE)AS [CAST_CREATED_DATE]
                        FROM    raw_user_table
                    )
                --Grab previous date for each distinct date
                , LAG_DATE AS (
                        SELECT      [CAST_CREATED_DATE]
                                    , LAG([CAST_CREATED_DATE],1, NULL) OVER(ORDER BY [CAST_CREATED_DATE]) AS [Previous_CREATED_DATE]
                        FROM        DATE_CONVERSION 
                        GROUP BY    [CAST_CREATED_DATE]
                )
                , DATE_DIFFERENCE AS (
                --Take difference between the two dates
                    SELECT      *
                                , DATEDIFF(d, [Previous_CREATED_DATE],[CAST_CREATED_DATE]) AS [Days_Difference]
                    FROM        LAG_DATE
                )
                --What are the gaps?
                SELECT      *
                FROM        DATE_DIFFERENCE
                ORDER BY    [Days_Difference] DESC
                --There are some large gaps. Would need to confirm if this is expected or missing data.


        --iii. BIRTH_DATE - datetime2
            --a. Confirm any conversion errors using datetime2
                SELECT BIRTH_DATE
                        , TRY_CAST(BIRTH_DATE AS DATETIME2) AS [CAST_BIRTH_DATE]
                FROM    raw_user_table
                --WHERE   TRY_CAST(BIRTH_DATE AS DATETIME2) IS NULL
                --No information is lost using this datatype.
                --TRY_CAST is never NULL, therefore conversion is successful.
          
            --b. Any outliers?
                --Make conversion to date (use date instead of date time because want to investigates dates, not times)        
                WITH DECIMAL_CONVERSION AS (
                    SELECT CAST(BIRTH_DATE AS DATETIME2)AS [CAST_BIRTH_DATE]
                    FROM    raw_user_table
                )
                --See the spread
                SELECT      [CAST_BIRTH_DATE]
                            , COUNT(1)
                FROM        DECIMAL_CONVERSION
                GROUP BY    [CAST_BIRTH_DATE]
                ORDER BY    [CAST_BIRTH_DATE]
                --There is data pre-1930 which seems unlikely and post-2010. Would need to confirm these dates and what this field means.
                --1/1/1900 - 4/3/2022

            --c. Any large gaps between the min and max date?
                --Going to see if any gaps between the previous date and current date of more than 1
                --Make conversion to date (use date instead of date time because want to investigates dates, not times)        
                WITH DATE_CONVERSION AS (
                        SELECT CAST(BIRTH_DATE AS DATE)AS [CAST_BIRTH_DATE]
                        FROM    raw_user_table
                    )
                --Grab previous date for each distinct date
                , LAG_DATE AS (
                        SELECT      [CAST_BIRTH_DATE]
                                    , LAG([CAST_BIRTH_DATE],1, NULL) OVER(ORDER BY [CAST_BIRTH_DATE]) AS [Previous_BIRTH_DATE]
                        FROM        DATE_CONVERSION 
                        GROUP BY    [CAST_BIRTH_DATE]
                )
                , DATE_DIFFERENCE AS (
                --Take difference between the two dates
                    SELECT      *
                                , DATEDIFF(d, [Previous_BIRTH_DATE],[CAST_BIRTH_DATE]) AS [Days_Difference]
                    FROM        LAG_DATE
                )
                --What are the gaps?
                SELECT      *
                FROM        DATE_DIFFERENCE
                ORDER BY    [Days_Difference] DESC
                --There are some large gaps. This goes along with the information above.
                --The higher gaps are for really old or really new "users".

            --d. Investigation of NULLs and Blanks
                SELECT      *
                FROM        raw_user_table
                WHERE       BIRTH_DATE IS NULL
                        OR BIRTH_DATE = ''
                --3675
                --Could be a voluntary disclosure

                --i. Is it missing only during a certain time period?
                    SELECT      MIN (CREATED_DATE) AS [MIN_CREATED_DATE]
                                , MAX(CREATED_DATE) AS [MAX_CREATED_DATE]
                    FROM        raw_user_table
                    WHERE       BIRTH_DATE IS NULL
                            OR BIRTH_DATE = ''
                    --Missing only in more recent years. Maybe it was made voluntary at some point.

                --ii. How many are missing other info?
                    SELECT SUM(CASE WHEN [STATE]         IS NULL OR [STATE]         = '' THEN 1 ELSE 0 END)    AS [STATE_NULLCount]
                        , SUM(CASE WHEN [LANGUAGE]      IS NULL OR [LANGUAGE]      = '' THEN 1 ELSE 0 END)    AS [LANGUAGE_NULLCount]
                        , SUM(CASE WHEN GENDER          IS NULL OR GENDER          = '' THEN 1 ELSE 0 END)    AS [GENDER_NULLCount]
                    FROM        raw_user_table
                    WHERE       BIRTH_DATE IS NULL
                            OR BIRTH_DATE = ''
                    --often missing other demographic information!


        --iv. STATE
            --a. Confirm consistent Length
                --State abbreviations are always two letters
                SELECT      LEN([STATE]) AS [Field_Length]
                            , COUNT(1) AS [TotalRows]
                FROM        raw_user_table
                GROUP BY    LEN([STATE])
                ORDER BY    TotalRows DESC
                --has consistent length

            --b.Any fake States?
                SELECT      DISTINCT [STATE]
                FROM        raw_user_table
                ORDER BY    [State]
                --There is 53 - 50 states + NULL + DC + PR. Confirmed externally.

            --c. Any less common states?
                SELECT      [STATE]
                            , COUNT(1) AS [TotalRows]
                FROM        raw_user_table
                GROUP BY    [STATE]
                ORDER BY    TotalRows DESC
                -- There is a big range in number of users from states, TX has the most and smaller/less populated states have fewer (like Vermont/Wyoming).
                -- See the Python Graph “03_Number of Records per State”.

            --d. Investigation of NULLs and Blanks
                SELECT      *
                FROM        raw_user_table
                WHERE       [STATE] IS NULL
                        OR [STATE] = ''
                --4812
                --Could be a voluntary disclosure

                --i. Is it missing only during a certain time period?
                    SELECT      MIN (CREATED_DATE) AS [MIN_CREATED_DATE]
                                , MAX(CREATED_DATE) AS [MAX_CREATED_DATE]
                    FROM        raw_user_table
                    WHERE       [STATE] IS NULL
                            OR [STATE]= ''
                    --Missing over the entire range. Could be voluntary disclosure.

                --ii. How many are missing other info?
                    SELECT SUM(CASE WHEN [BIRTH_DATE]    IS NULL OR [BIRTH_DATE]    = '' THEN 1 ELSE 0 END)    AS [BIRTH_DATE_NULLCount]
                        , SUM(CASE WHEN [LANGUAGE]      IS NULL OR [LANGUAGE]      = '' THEN 1 ELSE 0 END)    AS [LANGUAGE_NULLCount]
                        , SUM(CASE WHEN GENDER          IS NULL OR GENDER          = '' THEN 1 ELSE 0 END)    AS [GENDER_NULLCount]
                    FROM        raw_user_table
                    WHERE       [STATE] IS NULL
                            OR [STATE] = ''
                    --often missing other demographic information!


        --v. Languge
           --a. How common is each language? Any incorrect values?
                SELECT      [LANGUAGE]
                            , COUNT(1) AS [TotalRows]
                FROM        raw_user_table
                GROUP BY    [LANGUAGE]
                ORDER BY    TotalRows DESC
                --Only two options. es-419 is spanish spoken in Latin America/Caribbean.

            --b. Investigation of NULLs and Blanks
                SELECT      *
                FROM        raw_user_table
                WHERE       [LANGUAGE] IS NULL
                        OR [LANGUAGE] = ''
                --30508
                --Could be voluntary disclosure.

                --i. Is it missing only during a certain time period?
                    SELECT      MIN (CREATED_DATE) AS [MIN_CREATED_DATE]
                                , MAX(CREATED_DATE) AS [MAX_CREATED_DATE]
                    FROM        raw_user_table
                    WHERE       [LANGUAGE] IS NULL
                            OR [LANGUAGE] = ''
                    --Missing over the entire range. 

                --ii. How many are missing other info?
                    SELECT SUM(CASE WHEN [BIRTH_DATE]    IS NULL OR [BIRTH_DATE]    = '' THEN 1 ELSE 0 END)    AS [BIRTH_DATE_NULLCount]
                        , SUM(CASE WHEN [STATE]        IS NULL OR [STATE]        = '' THEN 1 ELSE 0 END)    AS [GENDER_NULLCount]
                        , SUM(CASE WHEN GENDER          IS NULL OR GENDER          = '' THEN 1 ELSE 0 END)    AS [GENDER_NULLCount]
                    FROM        raw_user_table
                    WHERE       [LANGUAGE] IS NULL
                            OR [LANGUAGE] = ''
                    --often missing other demographic information!


        --vi. Gender
           --a. How common is each gender? Any incorrect values?
            SELECT      [GENDER]
                        , COUNT(1) AS [TotalRows]
            FROM        raw_user_table
            GROUP BY    [GENDER]
            ORDER BY    TotalRows DESC
            --It is missing some standardization, i.e. there is both "prefer_not_to_say" and "Prefer not to say" that could bed be combined.

            --b. Investigation of NULLs and Blanks
                SELECT      *
                FROM        raw_user_table
                WHERE       [GENDER] IS NULL
                        OR [GENDER] = ''
                --5892
                --Shouldn't be NULL as has options for "prefer not to say". Points to missing data, though could still be not a required option.

                --i. Is it missing only during a certain time period?
                    SELECT      MIN (CREATED_DATE) AS [MIN_CREATED_DATE]
                                , MAX(CREATED_DATE) AS [MAX_CREATED_DATE]
                    FROM        raw_user_table
                    WHERE       [GENDER] IS NULL
                            OR [GENDER] = ''
                    --Missing over the entire range. 

                --ii. How many are missing other info?
                    SELECT SUM(CASE WHEN [BIRTH_DATE]    IS NULL OR [BIRTH_DATE]    = '' THEN 1 ELSE 0 END)    AS [BIRTH_DATE_NULLCount]
                        , SUM(CASE WHEN [STATE]        IS NULL OR [STATE]        = '' THEN 1 ELSE 0 END)    AS [GENDER_NULLCount]
                        , SUM(CASE WHEN [LANGUAGE]      IS NULL OR [LANGUAGE]      = '' THEN 1 ELSE 0 END)    AS [LANGUAGE_NULLCount]
                    FROM        raw_user_table
                    WHERE       [GENDER] IS NULL
                            OR [GENDER] = ''
                    --often missing other demographic information!


--3. Products Table
    SELECT TOP (100) *
    FROM raw_products_table

    /* GENERAL QUESTION: WHAT DO FIELDS MEAN?*/
    --Missing product name

    --a. Check NULLs
        --For each field, sum the total number of times the field is NULL or blank.
       SELECT SUM(CASE WHEN CATEGORY_1           IS NULL OR CATEGORY_1                 = '' THEN 1 ELSE 0 END)    AS [CATEGORY_1_NULLCount]
            , SUM(CASE WHEN CATEGORY_2           IS NULL OR CATEGORY_2                 = '' THEN 1 ELSE 0 END)    AS [CATEGORY_2_NULLCount]
            , SUM(CASE WHEN CATEGORY_3           IS NULL OR CATEGORY_3                 = '' THEN 1 ELSE 0 END)    AS [CATEGORY_3_NULLCount]
            , SUM(CASE WHEN CATEGORY_4           IS NULL OR CATEGORY_4                 = '' THEN 1 ELSE 0 END)    AS [CATEGORY_4_NULLCount]
            , SUM(CASE WHEN [MANUFACTURER]      IS NULL OR [MANUFACTURER]     = '' THEN 1 ELSE 0 END)    AS [MANUFACTURER_NULLCount]
            , SUM(CASE WHEN [BRAND]         IS NULL OR [BRAND]         = '' THEN 1 ELSE 0 END)    AS [BRAND_NULLCount]
            , SUM(CASE WHEN BARCODE         IS NULL OR BARCODE          = '' THEN 1 ELSE 0 END)          AS [BARCODE_NULLCount]
       , COUNT(1) FROM  raw_products_table
        --There is data missing in all columns, with the highest percentage in Category_4 (778093/845552 or 92%) and Manufacturer/Brand (27%)
        --Category_4 missing the most probably makes sense as some products don't need additional specifications, assuming 1-4 gets more specific as number increases.
        --The most concerning is BARCODE because there is no way to trace data back to the transaction data.
        --Manufacturer and brand could be linked with their missingness as the counts are almost the same.
        --Will check if any pattern to when these are NULL/blank below.

    --b. Individual Fields Investigation

        --i. CATEGORY_1 - nvarchar
            --a. Are the categories standardized?
                SELECT      [CATEGORY_1]
                            , COUNT(1) AS [TotalRows]
                FROM        raw_products_table
                GROUP BY    [CATEGORY_1]
                ORDER BY    [CATEGORY_1]
                --additional information on "Needs Review" category needed. Should treat as NULLs
                --Otherwise, it appears standardized.

                --i. Needs Review
                SELECT      *
                FROM        raw_products_table
                WHERE       CATEGORY_1 = 'Needs Review'

                    --a. Who is driving the Needs Review?
                    SELECT      MANUFACTURER
                                , COUNT(1) AS [TotalRows]
                    FROM        raw_products_table
                    WHERE       CATEGORY_1 = 'Needs Review'
                    GROUP BY    MANUFACTURER
                    ORDER BY    TotalRows DESC 
                    --seems to be multiple manufacturers and brands but all look to be drink/food
               
            --b. Outliers in terms of quantity of rows?
                SELECT      [CATEGORY_1]
                            , COUNT(1) AS [TotalRows]
                FROM        raw_products_table
                GROUP BY    [CATEGORY_1]
                ORDER BY    [TotalRows] DESC
                --Health & Wellness and Snacks are way higher in count of products
                --Would want more information on what products this data represents to know if it makes sense to have more of these
                --See the Python Graph “04_Number of Records per Category 1”.

                --i. Example: Health & Wellness
                    SELECT        Category_1
                                , Category_2
                                , COUNT(1) AS [TotalRows]
                    FROM        raw_products_table
                    WHERE       CATEGORY_1 = 'Health & Wellness'
                    GROUP BY    Category_1
                                , Category_2
                    ORDER BY    [TotalRows] DESC
                    --lots of medicines and haircare


            --c. Investigation of NULLs and Blanks
                SELECT      *
                FROM        raw_products_table
                WHERE       [Category_1] IS NULL
                        OR [Category_1] = ''
                --seems to be multiple manufacturers and brands
                --There is never an instance where CATEGORY_2/3/4 is not NULL and CATEGORY_1 is NULL. So at the minimum, all other products have CATEGORY_1.

                    --i. Who is driving the NULLs?
                    SELECT      MANUFACTURER
                                , COUNT(1) AS [TotalRows]
                    FROM        raw_products_table
                    WHERE       [Category_1] IS NULL
                            OR [Category_1] = ''
                    GROUP BY    MANUFACTURER
                    ORDER BY    TotalRows DESC 
                    --seems to be multiple manufacturers and brands but all look to be drink/food
                    --same companies that have the Needs Review above

            --d. Relationship of all the categories
                SELECT      DISTINCT CATEGORY_1
                            , CATEGORY_2
                            , CATEGORY_3
                            , CATEGORY_4
                FROM       raw_products_table
                ORDER BY    CATEGORY_1
                            , CATEGORY_2
                            , CATEGORY_3
                            , CATEGORY_4   
                --Appears to get more specific with each category number.
                --Beyond Category 1, appears that if the category # is NULL, it doesn't have a more specific category to fit into.
                    --We can't confirm this because we don't have product names to tell us what the products are and if could fit into a category.
                --We won't investigate the NULLs in CATEGORY_2 - CATEGORY_4 because of this pattern.


        --ii. CATEGORY_2 - nvarchar
            --a. Are the categories standardized?
                SELECT      [CATEGORY_2]
                            , COUNT(1) AS [TotalRows]
                FROM        raw_products_table
                GROUP BY    [CATEGORY_2]
                ORDER BY    [CATEGORY_2]
                --Seems to be standardized, no variations

            --b. Can the same CATEGORY_2 appear in multiple CATEGORY_1?
                SELECT      [CATEGORY_2]
                            , COUNT(DISTINCT CATEGORY_1) AS [Unique_CATEGORY_1]
                FROM        raw_products_table
                GROUP BY    [CATEGORY_2]
                ORDER BY    [Unique_CATEGORY_1] DESC
                --Nope, this is good to see because each category shouldn't (in theory, would need to confirm with client) have overlapping category_2 with other category_1.


        --iii. CATEGORY_3 - nvarchar

            --a. Are the categories standardized?
            SELECT      [CATEGORY_3]
                        , COUNT(1) AS [TotalRows]
            FROM        raw_products_table
            GROUP BY    [CATEGORY_3]
            ORDER BY    [CATEGORY_3]
            --Seems to be standardized, no variations

            --b. Can the same CATEGORY_3 appear in multiple CATEGORY_2?
            SELECT      [CATEGORY_3]
                        , COUNT(DISTINCT CATEGORY_2) AS [Unique_CATEGORY_2]
            FROM        raw_products_table
            GROUP BY    [CATEGORY_3]
            ORDER BY    [Unique_CATEGORY_2] DESC
            --Only SODA

                --i. Example: Soda
                SELECT      DISTINCT CATEGORY_1, CATEGORY_2
                FROM        raw_products_table
                WHERE       CATEGORY_3 = 'Soda'
                --This is okay. There is a categories for restaurant vs. non-restuarant soda which make sense.


        --iv. CATEGORY_4 - nvarchar

            --a. Are the categories standardized?
            SELECT      [CATEGORY_4]
                        , COUNT(1) AS [TotalRows]
            FROM        raw_products_table
            GROUP BY    [CATEGORY_4]
            ORDER BY    [CATEGORY_4]
            --Seems to be standardized, no variations

            --b. Can the same CATEGORY_4 appear in multiple CATEGORY_3?
            SELECT      [CATEGORY_4]
                        , COUNT(DISTINCT CATEGORY_3) AS [Unique_CATEGORY_3]
            FROM        raw_products_table
            GROUP BY    [CATEGORY_4]
            ORDER BY    [Unique_CATEGORY_3] DESC
            --Nope, this is good to see.


        --v. MANUFACTURER - nvarchar

            --a. Confirm MANUFACTURER is standardized, i.e. does the same MANUFACTURER  appear under multiple spellings, variations, etc.
                SELECT      DISTINCT MANUFACTURER 
                FROM        raw_products_table
                ORDER BY    MANUFACTURER 
                --Quick check shows standardization. Could spend more time here if needed.
                --Need more information on "Placeholder Manufacturer" -- essentially NULL but in case it is actually one manufacturer, will leave it.

                    --i. Example: Placehold Manufacturer
                        SELECT      *
                        FROM        raw_products_table
                        WHERE       MANUFACTURER = 'PLACEHOLDER MANUFACTURER'
                        --lots of variation in categories and brands

            --b. Investigate ones that have non-alphanumeric characters
                SELECT      DISTINCT MANUFACTURER 
                FROM        raw_products_table
                WHERE       PATINDEX('%[^a-zA-Z0-9 ]%',MANUFACTURER ) >= 1  --searching for any non-alphnumeric columns
                ORDER BY    MANUFACTURER 
                --Nothing very obvious. Could spend more time here if needed.

            --c. Investigate foreign characters         
                --Look for non-ASCII characters
                SELECT      DISTINCT MANUFACTURER  AS [StoreName]
                FROM        raw_products_table
                WHERE       MANUFACTURER COLLATE Latin1_General_BIN LIKE '%[^ -~]%';                     
                --Do not appear like this in CSV. Would need to use UTF codepage (unable to specify bulk insert codepage in Linux as I am using to access SQL server on personal computer)


            --d. Any large variance in number of products by manufacturer that could point to missing data?
                SELECT      MANUFACTURER
                            , COUNT(1) AS [TotalRows]                          
                FROM        raw_products_table
                GROUP BY    MANUFACTURER
                ORDER BY    TotalRows DESC
                --Would need more information on the size of the companies to know this.
                
            --e. Investigation of NULLs and Blanks
                SELECT      *
                FROM        raw_products_table
                WHERE       [MANUFACTURER] IS NULL
                        OR [MANUFACTURER] = ''

                --i. What products is driving the NULLs?
                    SELECT      CATEGORY_1
                                , CATEGORY_2
                                , COUNT(1) AS [TotalRows]
                    FROM       raw_products_table
                    WHERE       [MANUFACTURER] IS NULL
                            OR [MANUFACTURER] = ''
                    GROUP BY    CATEGORY_1
                                , CATEGORY_2
                    ORDER BY    [TotalRows] DESC
                    --It is snack and health & wellness which makes sense because they are the two biggest categories

                --ii. Is the missingness tied to the missingess of brand?
                    --Put rows into categories based on which fields it is missing
                    SELECT      CASE WHEN (MANUFACTURER IS NULL OR MANUFACTURER = '') AND (BRAND IS NULL OR BRAND = '') THEN 'Both Missing'
                                    WHEN (MANUFACTURER IS NULL OR MANUFACTURER = '') OR (BRAND IS NULL OR BRAND = '') THEN 'Only One is Missing'
                                    WHEN MANUFACTURER IS NOT NULL AND MANUFACTURER <> '' AND BRAND IS NOT NULL AND BRAND <> '' THEN 'Neither Missing'
                                    ELSE NULL END AS [Group]
                                , COUNT(1) AS [TotalRows]
                    FROM        raw_products_table
                    GROUP BY    CASE WHEN (MANUFACTURER IS NULL OR MANUFACTURER = '') AND (BRAND IS NULL OR BRAND = '') THEN 'Both Missing'
                                    WHEN (MANUFACTURER IS NULL OR MANUFACTURER = '') OR (BRAND IS NULL OR BRAND = '') THEN 'Only One is Missing'
                                    WHEN MANUFACTURER IS NOT NULL AND MANUFACTURER <> '' AND BRAND IS NOT NULL AND BRAND <> '' THEN 'Neither Missing'
                                    ELSE NULL END 
                    --Yes, they are almost exclusively missing together.

                    --a. Which are missing only one?
                        SELECT      *
                        FROM        raw_products_table
                        WHERE     ((MANUFACTURER IS NULL OR MANUFACTURER = '') AND BRAND IS NOT NULL AND BRAND <> '') --Missing Manufacturer
                                OR ((MANUFACTURER IS NOT NULL AND MANUFACTURER <> '') AND (BRAND IS NULL OR BRAND = '') ) --Missing Brand
                        
                        --i. Does the Brand "Listerine" ever have a manufacturer?
                            SELECT      *
                            FROM        raw_products_table
                            WHERE       Brand = 'Listerine'
                            --Yes, so why is the manufacturer NULL for the two above.


        --vi. BRAND - nvarchar

            --a. Confirm BRAND is standardized, i.e. does the same BRAND  appear under multiple spellings, variations, etc.
                SELECT      DISTINCT BRAND 
                FROM        raw_products_table
                ORDER BY    BRAND 
                --Very Quick check shows standardization. Could spend more time here if needed.

            --b. Investigate ones that have non-alphanumeric characters
                SELECT      DISTINCT BRAND 
                FROM        raw_products_table
                WHERE       PATINDEX('%[^a-zA-Z0-9 ]%',BRAND ) >= 1  --searching for any non-alphnumeric columns
                ORDER BY    BRAND 
                --Nothing very obvious. Could spend more time here if needed.

            --c. Investigate foreign characters         
                --Look for non-ASCII characters
                SELECT      DISTINCT BRAND  AS [StoreName]
                FROM        raw_products_table
                WHERE       BRAND COLLATE Latin1_General_BIN LIKE '%[^ -~]%';                     
                --Do not appear like this in CSV. Would need to use UTF codepage (unable to specify bulk insert codepage in Linux as I am using to access SQL server on personal computer)


        --vii. BARCODE - integer
            SELECT BARCODE
                    , TRY_CAST(BARCODE AS BIGINT) AS [CAST_BARCODE]
            FROM    raw_products_table
            WHERE   TRY_CAST(BARCODE AS BIGINT) IS NULL
                --ignore rows that don't have a barcode
                AND BARCODE IS NOT NULL
            --No information is lost using this datatype.
            --TRY_CAST is never NULL, therefore conversion is successful.

            --b. Length Comparison
                SELECT      LEN(BARCODE) AS [Field_Length]
                            , COUNT(1) AS [TotalRows]
                FROM        raw_products_table
                GROUP BY    LEN(BARCODE)
                ORDER BY    TotalRows DESC
                --length can vary like in the transaction table.

                --i. Can it vary within a manufacturer/brand?
                    --Does a manufacturer/brand ever have more than 1 barcode length?
                    SELECT      MANUFACTURER
                                , BRAND
                                , COUNT(DISTINCT LEN(BARCODE)) AS [UniqueField_Length]
                    FROM        raw_products_table
                    GROUP BY    MANUFACTURER
                                , BRAND
                    HAVING      COUNT(DISTINCT LEN(BARCODE)) > 1
                    --Yes, some manufacturer/brand do.

            --c. Investigation of NULLs and Blanks
                SELECT      *
                FROM        raw_products_table
                WHERE       BARCODE = ''
                        OR BARCODE IS NULL

                --i. What is the category for these rows?
                SELECT      CATEGORY_1
                            , COUNT(1) AS [TotalRows]
                FROM        raw_products_table
                WHERE       BARCODE IS NULL  
                        OR BARCODE = ''
                GROUP BY    CATEGORY_1
                --Across a few different categories.

                --ii. What is the manufacturer?
                SELECT      MANUFACTURER
                            , COUNT(1) AS [TotalRows]
                FROM        raw_products_table
                WHERE       BARCODE IS NULL  
                        OR BARCODE = ''
                GROUP BY    MANUFACTURER
                ORDER BY    [TotalRows] DESC
                --Across a handful of manufacturers

               
    --c. What represents a unique record in the transaction table?

            --i. Is it just barcode for non-NULLs?
                SELECT      BARCODE
                            , COUNT(1) AS [TotalRows]
                FROM        raw_products_table
                GROUP BY    BARCODE
                HAVING      COUNT(1) > 1 
                --Only NULL barcode plus two actual barcodes have multiple records?

                --a. Example of multiple records for barcode
                SELECT      *
                FROM        raw_products_table
                WHERE       BARCODE IN ('052336919068','017000329260')
                --They do have different brands!

            --ii. What other fields make a unique record?

                --a. Add MANUFACTURER/BRAND
                SELECT      BARCODE
                            , MANUFACTURER
                            , BRAND
                            , COUNT(1) AS [TotalRows]
                FROM        raw_products_table
                GROUP BY    BARCODE
                            , MANUFACTURER
                            , BRAND
                HAVING      COUNT(1) > 1 
                --Still duplication with NULLs  

                --b. Add MANUFACTURER/BRAND, categories
                SELECT      BARCODE
                            , MANUFACTURER
                            , BRAND
                            , CATEGORY_1
                            , CATEGORY_2
                            , CATEGORY_3
                            , CATEGORY_4
                            , COUNT(1) AS [TotalRows]
                FROM        raw_products_table
                GROUP BY    BARCODE
                            , MANUFACTURER
                            , BRAND
                            , CATEGORY_1
                            , CATEGORY_2
                            , CATEGORY_3
                            , CATEGORY_4
                HAVING      COUNT(1) > 1 
                --Still duplication with NULLs  
                --There is no unique identifier due the presence of NULL barcodes. Without these NULL barcodes, BARCODE is the unique identifier.


            --iii. Will there be duplication once convert BARCODE to int?
                WITH CONVERSION AS (
                    SELECT      CAST(NULLIF(BARCODE,'') AS BIGINT) AS [CAST_BARCODE]
                    FROM        raw_products_table
                )
                SELECT      CAST_BARCODE
                            , COUNT(1) AS [TotalRows]
                FROM        CONVERSION
                GROUP BY    CAST_BARCODE
                HAVING      COUNT(1) > 1
                ORDER BY    [TotalRows] DESC
                --Yes.

                --a. Does the information change between barcodes?
                --DROP TABLE IF EXISTS #Duplicated_Barcode;
                    WITH CONVERSION AS (
                        SELECT      *
                                    , CAST(NULLIF(BARCODE,'') AS BIGINT) AS [CAST_BARCODE]
                        FROM        raw_products_table
                    )
                    --get unique combinations of categories, manufacturer, and brand 
                    , UNIQUE_COMBOS AS (
                        SELECT      DISTINCT CATEGORY_1
                                    , CATEGORY_2
                                    , CATEGORY_3
                                    , CATEGORY_4
                                    , MANUFACTURER
                                    , BRAND
                                    , [CAST_BARCODE]
                        FROM        CONVERSION
                    )
                    --See which still have multiple lines
                    SELECT      CAST_BARCODE
                                , COUNT(1) AS [TotalRows]
                    INTO        #Duplicated_Barcode
                    FROM        UNIQUE_COMBOS
                    WHERE       CAST_BARCODE IS NOT NULL
                    GROUP BY    CAST_BARCODE
                    HAVING      COUNT(1) > 1
                    --Yes. Need to investigate further.
                    --(27 rows affected)

                --b. Review Duplicated Barcodes
                --Subset larger products data to barcodes of interest
                    WITH CONVERSION AS (
                        SELECT      *
                                    , CAST(NULLIF(BARCODE,'') AS BIGINT) AS [CAST_BARCODE]
                        FROM        raw_products_table
                    )
                    SELECT      *
                    FROM        CONVERSION con
                    INNER JOIN  #Duplicated_Barcode dup
                                ON con.CAST_BARCODE = dup.CAST_BARCODE
                    ORDER BY    con.CAST_BARCODE
                    --Some appear to be the same, others are different.
                    --We will note the duplication and continue


--END OF SCRIPT