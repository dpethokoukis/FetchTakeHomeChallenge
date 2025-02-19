/***********************************************
Name:           Deirdre Pethokoukis
Script Name:    02_FetchTakeHome_DataQualityChecks_DataIssuesOnly
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

    --a. Row Counts
        SELECT      COUNT(1) AS [TotalRows]
        FROM        raw_transaction_table
        --50,000


    --b. Unique Record
        --What represents a unique record in the transaction table?
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


    --c. Data Types

        --1. PURCHASE_DATE
            --Ever any time information?
            --If the hour is ever different from 0, then we know there is time information.
            SELECT      DATEPART(hh, CAST(PURCHASE_DATE AS DATETIME2)) AS [HourofTime]
                        , COUNT(1) AS [TotalRows]
            FROM        raw_transaction_table
            GROUP BY    DATEPART(hh, CAST(PURCHASE_DATE AS DATETIME2)) 
            ORDER BY    DATEPART(hh, CAST(PURCHASE_DATE AS DATETIME2)) 
            --Never any time data. could save space by saving as a date, but will use that datetype provided in instructions (datetime2).

        --2. SCAN_DATE 
            SELECT      SCAN_DATE
            FROM        raw_transaction_table
            --Why does the name say only date but also has times? Would be better to all it Scan_DateTime.


    --d. Clean Values

        --1. STORE_NAME
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

        --2. BARCODE
            --Length Comparison
                SELECT      LEN(BARCODE) AS [Field_Length]
                            , COUNT(1) AS [TotalRows]
                FROM        raw_transaction_table
                GROUP BY    LEN(BARCODE)
                ORDER BY    TotalRows DESC
                --length can vary. Why?


        --3. FINAL_QUANTITY
            --Check if data after the decimal
            SELECT      *
            FROM        raw_transaction_table
            WHERE       RIGHT(FINAL_QUANTITY,2) <> '00' --if not equal to 00, then has decimal values.
                    AND FINAL_QUANTITY <> 'zero'
            --yes


    --e. Missing Values

        --For each field, sum the total number of times the field is NULL or blank.
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

        --1. BARCODE
            --a. What is the quantity for these rows?
                SELECT      FINAL_QUANTITY
                            , COUNT(1) AS [TotalRows]
                FROM        raw_transaction_table
                WHERE       BARCODE IS NULL  
                           OR BARCODE = ''
                GROUP BY    FINAL_QUANTITY
                ORDER BY    FINAL_QUANTITY
                    --Quantity varies, not just zero.

            --b. What is final_sale for these rows?
                SELECT      FINAL_SALE
                            , COUNT(1) AS [TotalRows]
                FROM        raw_transaction_table
                WHERE       BARCODE IS NULL  
                        OR BARCODE = ''
                GROUP BY    FINAL_SALE
                ORDER BY    FINAL_SALE
                --Can be blank but some have values.

            --c. Is it missing only during a certain time period?
                SELECT      MIN (PURCHASE_DATE) AS [MIN_PURCHASE_DATE]
                            , MAX(PURCHASE_DATE) AS [MAX_PURCHASE_DATE]
                            , MIN (SCAN_DATE) AS [MIN_SCAN_DATE]
                            , MAX(SCAN_DATE) AS [MAX_SCAN_DATE]
                FROM        raw_transaction_table
                WHERE       FINAL_SALE IS NULL  
                        OR FINAL_SALE = ''
                --Takes place over the entire range of purchases/scan dates.
     
        --2. FINAL_SALE
            --Is it missing only during a certain time period?
            SELECT      MIN (PURCHASE_DATE) AS [MIN_PURCHASE_DATE]
                        , MAX(PURCHASE_DATE) AS [MAX_PURCHASE_DATE]
                        , MIN (SCAN_DATE) AS [MIN_SCAN_DATE]
                        , MAX(SCAN_DATE) AS [MAX_SCAN_DATE]
            FROM        raw_transaction_table
            WHERE       FINAL_SALE IS NULL  
                    OR FINAL_SALE = ''
            --Takes place over the entire range of purchase/scan dates.


    --f. Duplicated Values

        --1. BARCODE
            --Is the BARCODE always the same within the receipt_id?
            --USE ISNULL(NULLIF(BARCODE,''),0)) to convert all blanks to NULLs, and then all NULLs (and now blanks) to 0s so they are counted in the COUNT DISTINCT 
            SELECT  RECEIPT_ID
                   , COUNT(DISTINCT ISNULL(NULLIF(BARCODE,''),0)) AS [UniqueBARCODES]
                    , COUNT(ISNULL(NULLIF(BARCODE,''),0)) AS [TotalRows]
            FROM    raw_transaction_table
            GROUP BY RECEIPT_ID
            HAVING COUNT(DISTINCT ISNULL(NULLIF(BARCODE,''),0)) <> COUNT(1)
            --The number of rows is never the same as unique BARCODES (all 24,440 RECEIPTs have this charactersitic)
            --Why does the same BARCODE appears multiple times because there is a "FINAL_QUANTITY" field


    --g. Outliers

        --1. FINAL_QUANTITY
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

                --a. Example of 276
                    SELECT      *
                    FROM        raw_transaction_table
                    WHERE       TRY_CAST(FINAL_QUANTITY AS decimal(15,2)) = 276.00
                    --Would need more context to understand if this is valid or not

                    SELECT      *
                    FROM        raw_products_table
                    WHERE       BARCODE LIKE '%48001353664%'
                    --no information for barcode


        --2. FINAL_SALE
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

                --a. Example of over $200
                    SELECT      *
                    FROM        raw_transaction_table
                    WHERE       TRY_CAST(FINAL_SALE AS decimal(15,2)) > 200
                    --Would need more context to understand if this is valid or not, i.e. what the product is.


    --h. Others

        --1. BARCODE
            --Is there ever more than one BARCODE within a RECEIPT_ID?
            SELECT  RECEIPT_ID
                    , COUNT(DISTINCT BARCODE) AS [UniqueBarccodes]
                    , COUNT(1) AS [TotalRows]
            FROM    raw_transaction_table
            GROUP BY RECEIPT_ID
            HAVING COUNT(DISTINCT BARCODE) NOT IN (0,1)
            ---can have multiple BARCODES but not common - 329 instances


        --2. FINAL_SALES VS. FINAL_QUANTITY
            --a. How often is FINAL_QUANTITY zero and FINAL_SALE > 0?
                --If final sale is the final amount paid, would expect there to be a quantity
                SELECT       *
                FROM        raw_transaction_table
                WHERE   (    TRY_CAST(FINAL_QUANTITY AS decimal(15,2)) = 0
                        OR FINAL_QUANTITY = 'zero' )
                        AND TRY_CAST(FINAL_SALE AS decimal(15,2)) > 0
                --12,341 / 50,000 (25%), this is a high proportion

            --b. How often is FINAL_QUANTITY > 0 and FINAL_SALE is blank/zero/NULL
                --If final sale is the final amount paid, would expect there to be a quantity
                SELECT       *
                FROM        raw_transaction_table
                WHERE      TRY_CAST(FINAL_QUANTITY AS decimal(15,2)) > 0
                        AND (TRY_CAST(FINAL_SALE AS decimal(15,2)) = 0
                            OR FINAL_SALE IS NULL
                            OR FINAL_SALE = ''
                        )
                --12,821 / 50,000 (26%), this is a high proportion


--2. USERS TABLE

    --a. Row Counts
        SELECT      COUNT(1) AS [TotalRows]
        FROM        raw_user_table
        --100,000


    --b. Clean Values

        --1. GENDER
            --look at values
            SELECT      [GENDER]
                        , COUNT(1) AS [TotalRows]
            FROM        raw_user_table
            GROUP BY    [GENDER]
            ORDER BY    TotalRows DESC
            --It is missing some standardization, i.e. there is both "prefer_not_to_say" and "Prefer not to say" that could bed be combined.


    --c. Missing Values
    
        --For each field, sum the total number of times the field is NULL or blank.
       SELECT SUM(CASE WHEN ID              IS NULL OR ID              = '' THEN 1 ELSE 0 END)    AS [ID_NULLCount]
            , SUM(CASE WHEN CREATED_DATE    IS NULL OR CREATED_DATE    = '' THEN 1 ELSE 0 END)    AS [CREATED_DATE_NULLCount]
            , SUM(CASE WHEN BIRTH_DATE      IS NULL OR BIRTH_DATE      = '' THEN 1 ELSE 0 END)    AS [BIRTH_DATE_NULLCount]
            , SUM(CASE WHEN [STATE]         IS NULL OR [STATE]         = '' THEN 1 ELSE 0 END)    AS [STATE_NULLCount]
            , SUM(CASE WHEN [LANGUAGE]      IS NULL OR [LANGUAGE]      = '' THEN 1 ELSE 0 END)    AS [LANGUAGE_NULLCount]
            , SUM(CASE WHEN GENDER          IS NULL OR GENDER          = '' THEN 1 ELSE 0 END)    AS [GENDER_NULLCount]
        FROM  raw_user_table

        --1. CREATED_DATE
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

        --2. BIRTH_DATE
            --a. Is it missing only during a certain time period?
                SELECT      MIN (CREATED_DATE) AS [MIN_CREATED_DATE]
                            , MAX(CREATED_DATE) AS [MAX_CREATED_DATE]
                FROM        raw_user_table
                WHERE       BIRTH_DATE IS NULL
                        OR BIRTH_DATE = ''
                --Missing only in more recent years (2021+). Maybe it was made voluntary at some point.

            --b. How many are missing other info?
                SELECT SUM(CASE WHEN [STATE]         IS NULL OR [STATE]         = '' THEN 1 ELSE 0 END)    AS [STATE_NULLCount]
                    , SUM(CASE WHEN [LANGUAGE]      IS NULL OR [LANGUAGE]      = '' THEN 1 ELSE 0 END)    AS [LANGUAGE_NULLCount]
                    , SUM(CASE WHEN GENDER          IS NULL OR GENDER          = '' THEN 1 ELSE 0 END)    AS [GENDER_NULLCount]
                FROM        raw_user_table
                WHERE       BIRTH_DATE IS NULL
                        OR BIRTH_DATE = ''
                --often missing other demographic information!

            --c. Any large gaps between the min and max date of when users born?
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
                --There are some large gaps. This goes along with the information about outliers.
                --The higher gaps are for really old or really new "users".


        --3. STATE, LANGUAGE, GENDER
            --a. STATE
                SELECT      MIN (CREATED_DATE) AS [MIN_CREATED_DATE]
                            , MAX(CREATED_DATE) AS [MAX_CREATED_DATE]
                FROM        raw_user_table
                WHERE       [STATE] IS NULL
                        OR [STATE]= ''
                    --Missing over the entire range. Could be voluntary disclosure.

            --b. LANGUAGE
                SELECT      MIN (CREATED_DATE) AS [MIN_CREATED_DATE]
                            , MAX(CREATED_DATE) AS [MAX_CREATED_DATE]
                FROM        raw_user_table
                WHERE       [LANGUAGE] IS NULL
                        OR [LANGUAGE] = ''
                    --Missing over the entire range. 

            --c. GENDER
                SELECT      MIN (CREATED_DATE) AS [MIN_CREATED_DATE]
                            , MAX(CREATED_DATE) AS [MAX_CREATED_DATE]
                FROM        raw_user_table
                WHERE       [GENDER] IS NULL
                            OR [GENDER] = ''
                --Missing over the entire range. 


    --d. Outliers

        --1. BIRTH_DATE
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


--3. Products Table

    --a. Unique Record

        --a. Is it just barcode for non-NULLs?
            SELECT      BARCODE
                        , COUNT(1) AS [TotalRows]
            FROM        raw_products_table
            GROUP BY    BARCODE
            HAVING      COUNT(1) > 1 
            --Only NULL barcode plus two actual barcodes have multiple records?

            --i. Example of multiple records for barcode
            SELECT      *
            FROM        raw_products_table
            WHERE       BARCODE IN ('052336919068','017000329260')
            --They do have different brands!

        --b. What other fields make a unique record?

            --i. Add MANUFACTURER/BRAND
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

            --ii. Add MANUFACTURER/BRAND, categories
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


        --c. Will there be duplication once convert BARCODE to int?
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

                --i. Does the information change between barcodes?
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

                --ii. Review Duplicated Barcodes
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


    --b. Clean Values

        --1. CATEGORY_1
            --Are the categories standardized?
                SELECT      [CATEGORY_1]
                            , COUNT(1) AS [TotalRows]
                FROM        raw_products_table
                GROUP BY    [CATEGORY_1]
                ORDER BY    [CATEGORY_1]
                --additional information on "Needs Review" category needed. Should treat as NULLs
                --Otherwise, it appears standardized.

                --a. Needs Review
                SELECT      *
                FROM        raw_products_table
                WHERE       CATEGORY_1 = 'Needs Review'

                    --i. Who is driving the Needs Review?
                    SELECT      MANUFACTURER
                                , COUNT(1) AS [TotalRows]
                    FROM        raw_products_table
                    WHERE       CATEGORY_1 = 'Needs Review'
                    GROUP BY    MANUFACTURER
                    ORDER BY    TotalRows DESC 
                    --seems to be multiple manufacturers and brands but all look to be drink/food

        --2. MANUFACTURER
            -- Confirm MANUFACTURER is standardized, i.e. does the same MANUFACTURER  appear under multiple spellings, variations, etc.
                SELECT      DISTINCT MANUFACTURER 
                FROM        raw_products_table
                ORDER BY    MANUFACTURER 
                --Quick check shows standardization. Could spend more time here if needed.
                --Need more information on "Placeholder Manufacturer" -- essentially NULL but in case it is actually one manufacturer, will leave it.

                    --a. Example: Placehold Manufacturer
                        SELECT      *
                        FROM        raw_products_table
                        WHERE       MANUFACTURER = 'PLACEHOLDER MANUFACTURER'
                        --lots of variation in categories and brands

        --3. BARCODE
            --Length Comparison
                SELECT      LEN(BARCODE) AS [Field_Length]
                            , COUNT(1) AS [TotalRows]
                FROM        raw_products_table
                GROUP BY    LEN(BARCODE)
                ORDER BY    TotalRows DESC
                --length can vary like in the transaction table.


    --c. Missing Values

        --For each field, sum the total number of times the field is NULL or blank.
       SELECT SUM(CASE WHEN CATEGORY_1           IS NULL OR CATEGORY_1                 = '' THEN 1 ELSE 0 END)    AS [CATEGORY_1_NULLCount]
            , SUM(CASE WHEN CATEGORY_2           IS NULL OR CATEGORY_2                 = '' THEN 1 ELSE 0 END)    AS [CATEGORY_2_NULLCount]
            , SUM(CASE WHEN CATEGORY_3           IS NULL OR CATEGORY_3                 = '' THEN 1 ELSE 0 END)    AS [CATEGORY_3_NULLCount]
            , SUM(CASE WHEN CATEGORY_4           IS NULL OR CATEGORY_4                 = '' THEN 1 ELSE 0 END)    AS [CATEGORY_4_NULLCount]
            , SUM(CASE WHEN [MANUFACTURER]      IS NULL OR [MANUFACTURER]     = '' THEN 1 ELSE 0 END)    AS [MANUFACTURER_NULLCount]
            , SUM(CASE WHEN [BRAND]         IS NULL OR [BRAND]         = '' THEN 1 ELSE 0 END)    AS [BRAND_NULLCount]
            , SUM(CASE WHEN BARCODE         IS NULL OR BARCODE          = '' THEN 1 ELSE 0 END)          AS [BARCODE_NULLCount]
       , COUNT(1) FROM  raw_products_table

        --1. CATEGORY_1 
            SELECT      *
            FROM        raw_products_table
            WHERE       [Category_1] IS NULL
                    OR [Category_1] = ''
            --seems to be multiple manufacturers and brands
            --There is never an instance where CATEGORY_2/3/4 is not NULL and CATEGORY_1 is NULL. So at the minimum, all other products have CATEGORY_1.

                    --a. Who is driving the NULLs?
                    SELECT      MANUFACTURER
                                , COUNT(1) AS [TotalRows]
                    FROM        raw_products_table
                    WHERE       [Category_1] IS NULL
                            OR [Category_1] = ''
                    GROUP BY    MANUFACTURER
                    ORDER BY    TotalRows DESC 
                    --seems to be multiple manufacturers and brands but all look to be drink/food
                    --same companies that have the Needs Review above


        --2. MANUFACTURER
              --Is the missingness tied to the missingess of brand?
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
                    --Yes, they are almost exclusively missing together. Only 2 instances were not.

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

        --3. BARCODE
                --a. What is the category for these rows?
                SELECT      CATEGORY_1
                            , COUNT(1) AS [TotalRows]
                FROM        raw_products_table
                WHERE       BARCODE IS NULL  
                        OR BARCODE = ''
                GROUP BY    CATEGORY_1
                --Across a few different categories.

                --b. What is the manufacturer?
                SELECT      MANUFACTURER
                            , COUNT(1) AS [TotalRows]
                FROM        raw_products_table
                WHERE       BARCODE IS NULL  
                        OR BARCODE = ''
                GROUP BY    MANUFACTURER
                ORDER BY    [TotalRows] DESC
                --Across a handful of manufacturers


    --d. Outliers

        --1. CATEGORY_1
                SELECT      [CATEGORY_1]
                            , COUNT(1) AS [TotalRows]
                FROM        raw_products_table
                GROUP BY    [CATEGORY_1]
                ORDER BY    [TotalRows] DESC
                --Health & Wellness and Snacks are way higher in count of products
                --Would want more information on what products this data represents to know if it makes sense to have more of these
                --See the Python Graph “04_Number of Records per Category 1”.

                --a. Example: Health & Wellness
                    SELECT        Category_1
                                , Category_2
                                , COUNT(1) AS [TotalRows]
                    FROM        raw_products_table
                    WHERE       CATEGORY_1 = 'Health & Wellness'
                    GROUP BY    Category_1
                                , Category_2
                    ORDER BY    [TotalRows] DESC
                    --lots of medicines and haircare


--END OF SCRIPT