/***********************************************
Name:           Deirdre Pethokoukis
Script Name:    05_FetchTakeHome_SQLQueries
Database:       USE FetchTakeHome;
Purpose:        Answer open-ended and closed-ended 
                questions
*************************************************/

---------------------
-- DATABASE SET UP --
---------------------
--1. Specify database to use for script
    USE FetchTakeHome;

--------------------------
-- GENERAL LIMITATIONS  --
-- & ASSUMPTIONS        --
--------------------------

--1. Limitations
    --a. The transaction data only exists from 6/12/2024 - 9/8/2024. Therefore, this data is unable to provide any trends for longer periods of time and only provides results true of this time period.
    --b. There are not many matches between the USER/PRODUCT tables and the TRANSACTION data. Therefore, there is limited information about the products and users found
            --in the transaction table. However, if the data was more complete the queries created would still apply.
    --c. Any results apply to only data found in these datasets, i.e. to Fetch users. No statements about broader consumer trends can be made.

--2. Assumptions
    --a. FINAL_SALE represents the final amount spent by the customer for a given line item on the receipt, i.e. for a row in the table.
    --b. When using the master table, there is slight duplication as detailed in data issues. Efforts are taken to minimize duplication by using distinct values where possible.


--------------------
-- CLOSE-ENDED    --
--------------------

--i. What is the percentage of sales in the Health & Wellness category by generation?
    --a. Additional Assumptions: 
        --1. "by generation" is the generation of users by age - i.e. Gen X, Gen Z, etc.
        --2. "Percentage of Sales" is a the % of the sum of FINAL_SALE, rather than FINAL_QUANTITY.
            --a. Duplication may exist here as we are treating each line as distinct.
        --3. Generation are defined as (source: https://libguides.usc.edu/busdem/age):
            /*
            The Greatest Generation – born 1901-1924.
            The Silent Generation – born 1925-1945.
            The Baby Boomer Generation – born 1946-1964.
            Generation X – born 1965-1979.
            Millennials – born 1980-1994.
            Generation Z – born 1995-2012.
            Gen Alpha – born 2013 – 2025.
            */

    --b. Query to Solve
    --Get fields we need for our calculation -- FINAL_SALE and YEAR of BIRTH_DATE for users
    --Subset the data to the Health & Wellness Category
    WITH NEEDED_DATA AS (
        SELECT      FINAL_SALE
                    , CASE WHEN USERID_BIRTH_DATE IS NOT NULL THEN DATEPART(year,USERID_BIRTH_DATE) 
                        ELSE NULL END AS [Year_of_Birth] --extract the year from user's BIRTH_DATE (where exists)
        FROM        stage_master_table
        WHERE       BARCODE_CATEGORY_1 = 'Health & Wellness'
    )
    --Calculate generation of each user based on the year of BIRTH_DATE
    , GENERATIONS AS (
        SELECT      *
                    , CASE WHEN [Year_of_Birth] BETWEEN 1901 AND 1924 THEN 'The Greatest Generation'
                            WHEN [Year_of_Birth] BETWEEN 1925 AND 1945 THEN 'The Silent Generation'
                            WHEN [Year_of_Birth] BETWEEN 1946 AND 1964 THEN 'The Baby Boomer Generation'
                            WHEN [Year_of_Birth] BETWEEN 1965 AND 1979 THEN 'Generation X'
                            WHEN [Year_of_Birth] BETWEEN 1980 AND 1994 THEN 'Millenials'
                            WHEN [Year_of_Birth] BETWEEN 1995 AND 2012 THEN 'Generation Z'
                            WHEN [Year_of_Birth] BETWEEN 2013 AND 2025 THEN 'Gen Alpha'
                            WHEN [Year_of_Birth] IS NULL THEN 'Unknown' --for users without a BIRTH_DATE (i.e. BIRTH_DATE is NULL in USERS table) or without a matching record in USERS table.
                            ELSE 'Outside Range' END AS [Generation]    --any users who has a birthday outside of 1901 - 2025.
        FROM        NEEDED_DATA
    )
    --Sum FINAL_SALE by Generation and divide by total FINAL_SALE for Health & Wellness
    --sum for the single generation / sum across all generations  gives percentage per generation.
    SELECT      [GENERATION]      
                , (SUM(FINAL_SALE) / 
                    SUM(SUM(FINAL_SALE)) OVER()) AS GENERATION_PERCENTAGE_OF_TOTAL
            --Version of results that doesn't include the 'unknown' generation                       
                , (SUM(CASE WHEN GENERATION ='Unknown' THEN NULL ELSE FINAL_SALE END) / 
                   SUM(SUM(CASE WHEN GENERATION ='Unknown'  THEN NULL ELSE FINAL_SALE END)) OVER()) AS GENERATION_PERCENTAGE_OF_TOTAL_NoUnknown 
    FROM        GENERATIONS
    GROUP BY    [GENERATION]
    ORDER BY    [GENERATION]
/*
GENERATION	        GENERATION_PERCENTAGE_OF_TOTAL	    GENERATION_PERCENTAGE_OF_TOTAL_NoUnknown
Generation X	            0.001001	                                0.218812
Millenials	                0.001426	                                0.311768
The Baby Boomer Generation	0.002148	                                0.469418
Unknown	                    0.995424	                                NULL

Answer: When exlcuding users with an unknown BIRTH_DATE/age, the percentage of sales in the Health & Wellness category by generation is:
    Generation X - 21.9%, Millenials - 31.1%, and The Baby Boomer Generation - 47.0%. All other generations do not have sales in this category.

*/


-------------------
-- OPEN-ENDED   --
-------------------

--i. Who are Fetch’s power users?
    --a. Additional Assumptions:
        --1. The definition of a power user is based on two metrics:
            --a. App Loyalty: Users who use the app over a long period of time.
            --b. App Usage: Users who use the app consistently and scan many receipts.
        --2. The measure of "App Loyalty" is:
            --a. There are 14 weeks represented by the data
            --b. Week is defined as Sunday to Saturday based off the definition of a "week" in the DATEPART function in SQL Server.
            --c. Users who are in the top 1% of users based on number of weeks the app is used.
            --d. SCAN_DATE is used instead of PURCHASE_DATE as SCAN_DATE reflects when the app is used, regardless of when the items were purchased.
                --Therefore, it reflects when the decision is made to use the app.
        --3. The measure of "App Usage" is:
            --a. Users who are in the top 1% of users based on the number of receipts scanned.
            --b. This helps to weed out users who may scan once a week (App Loyalty) but only 1 receipt each time.
        --4. We are using the transaction table to avoid the duplication issue.
            --a. No fields are needed from the USER or PRODUCTS table.

    --b. Preliminary Step: How many unique weeks are in the dataset?
    --Use DATEPART function to get the unique week numbers represented by each SCAN_DATE
    SELECT      DISTINCT DATEPART(WEEK, SCAN_DATE) AS [Week_of_Year]
    FROM        stage_transaction_table
    ORDER BY    [Week_of_Year]
    --14

    --c. Query to Solve:
    --Get the data we need: USER_ID, week of SCAN_DATE, and RECEIPT_ID.
    --Get unique values to treat each receipt as one.
        WITH MIN_MAX_DATE AS (
            SELECT     DISTINCT USER_ID
                        ,DATEPART(WEEK, SCAN_DATE) AS [Week_of_Year]
                        , RECEIPT_ID
            FROM        stage_transaction_table
        )
        --Count the unique weeks and unique receipts for each USER_ID
        , COUNT_DATA AS (
            SELECT      USER_ID
                         , COUNT(DISTINCT [Week_of_Year]) AS [CountofWeeks]
                         , COUNT(DISTINCT RECEIPT_ID) AS [UniqueReceiptsScanned]
             FROM        MIN_MAX_DATE
            GROUP BY    USER_ID
         )
         --Use PERCENT_RANK() to get the percentile for weeks/receipts scanned
        , RANKED_USERS AS (
            SELECT      USER_ID
                        ,PERCENT_RANK() OVER (ORDER BY [CountofWeeks] DESC) AS [Percentile_Weeks]
                        ,PERCENT_RANK() OVER (ORDER BY [UniqueReceiptsScanned] DESC) AS [Percentile_Receipts]
            FROM        COUNT_DATA
        )
        --Identify users who are in the top 1% for both metrics
        SELECT      DISTINCT USER_ID
        FROM        RANKED_USERS 
        WHERE       [Percentile_Weeks] <= 0.01 
                AND [Percentile_Receipts]  <= 0.01 
        --215 "power users"

/*
Answer: Whe defining a "Power User" as being in the top 1% of app loyalty and app usage, then there 215 "power users"?
*/


--ii. Which is the leading brand in the Dips & Salsa category?
    --a. Additional Assumptions:
        --1. The definition of a leading brand will be calculated as:
            --a. Most Scanned Brand - This will show us which brand is scanned the most by Fetch users.
            --b. Another way to calculate would be by "Highest Sales" - which brand had the most money spent on them by Fetch users (i.e. sum of FINAL_PRICE across items sold by the brand). 
                    --This is not the method we are using.
        --2. The "Most Scanned Brand" is the number of unique receipts scanned for each brand.
            --a. The same receipt can be counted twice if user bought more than one item in the Dips & Salsa category that were different brands.
        --3. We are ignoring receipts without brand data.

    --b. Preliminary Step - What level category is "Dips & Salsa"?
        SELECT      DISTINCT BARCODE_CATEGORY_1
                    , BARCODE_CATEGORY_2
                    , BARCODE_CATEGORY_3
                    , BARCODE_CATEGORY_4
        FROM        stage_master_table
        WHERE       BARCODE_CATEGORY_1 = 'Dips & Salsa'
                OR  BARCODE_CATEGORY_2 = 'Dips & Salsa'
                OR  BARCODE_CATEGORY_3 = 'Dips & Salsa'
                OR  BARCODE_CATEGORY_4 = 'Dips & Salsa'
        --Category_2

    --c. Query to Solve:
    --Get fields we need for our calculation -- BRAND and RECEIPT_ID
    --Subset to Dips & Salsa category.
    --Get unique values to treat each brand/receipt as one.
    WITH NEEDED_DATA AS (
        SELECT      DISTINCT BARCODE_BRAND
                    , RECEIPT_ID
        FROM        stage_master_table
        WHERE       BARCODE_CATEGORY_2 = 'Dips & Salsa'
                AND BARCODE_BRAND IS NOT NULL  --don't need data without brand info
    )
    --Get count of receipts scanned per brand
    SELECT      BARCODE_BRAND
                , COUNT(1) AS [TotalReceipts]
    FROM        NEEDED_DATA
    GROUP BY    BARCODE_BRAND
    ORDER BY    TotalReceipts DESC
    --Tostitos

/*
Answer: When measuring the top brand as the brand with the most receipts scanned, then Tositos leads the Dips & Salsa Category.
*/


----------------------
-- OWN EXPLORATION  --
----------------------
--USED FOR EMAIL/COMMUNICATION WITH STAKEHOLDERS

--i. What is the top store scanned by generation?
    --a. Additional Assumptions: 
        --1. "by generation" is the generation of users by age - i.e. Gen X, Gen Z, etc.
        --2. "top store" is defined as the "most scanned store", or the most unique RECEIPT_IDs.
        --3. Generation are defined as (source: https://libguides.usc.edu/busdem/age):
            /*
            The Greatest Generation – born 1901-1924.
            The Silent Generation – born 1925-1945.
            The Baby Boomer Generation – born 1946-1964.
            Generation X – born 1965-1979.
            Millennials – born 1980-1994.
            Generation Z – born 1995-2012.
            Gen Alpha – born 2013 – 2025.
            */

    --b. Query to Solve
    --DROP TABLE IF EXISTS #Generation_Stores;
    --Get fields we need for our calculation -- RECEIPT_ID, STORE_NAME, and year of BIRTH_DATE
    --Subset to unique combinations to treat each receipt_ID as one.
    WITH NEEDED_DATA AS (
        SELECT      DISTINCT RECEIPT_ID
                    , STORE_NAME
                    , CASE WHEN USERID_BIRTH_DATE IS NOT NULL THEN DATEPART(year,USERID_BIRTH_DATE) 
                        ELSE NULL END AS [Year_of_Birth] --extract the year from user's BIRTH_DATE (where exists)
        FROM        stage_master_table
        WHERE       STORE_NAME IS NOT NULL
    )
    --Calculate generation of each user based on the year of BIRTH_DATE
    , GENERATIONS AS (
        SELECT      *
                    , CASE WHEN [Year_of_Birth] BETWEEN 1901 AND 1924 THEN 'The Greatest Generation'
                            WHEN [Year_of_Birth] BETWEEN 1925 AND 1945 THEN 'The Silent Generation'
                            WHEN [Year_of_Birth] BETWEEN 1946 AND 1964 THEN 'The Baby Boomer Generation'
                            WHEN [Year_of_Birth] BETWEEN 1965 AND 1979 THEN 'Generation X'
                            WHEN [Year_of_Birth] BETWEEN 1980 AND 1994 THEN 'Millenials'
                            WHEN [Year_of_Birth] BETWEEN 1995 AND 2012 THEN 'Generation Z'
                            WHEN [Year_of_Birth] BETWEEN 2013 AND 2025 THEN 'Gen Alpha'
                            WHEN [Year_of_Birth] IS NULL THEN 'Unknown' --for users without a BIRTH_DATE or don't have a matching USER record in USERS table
                            ELSE 'Outside Range' END AS [Generation]    --any users who has a birthday outside of 1901 - 2025
        FROM        NEEDED_DATA
    )
    --Count the number of receipts by store by generation
    , COUNT_STORES AS (
        SELECT       GENERATION
                    , STORE_NAME
                    , COUNT(1) AS [TotalReceipts]
        FROM        GENERATIONS
        GROUP BY    GENERATION
                    , STORE_NAME
    )
    --Use the RANK() function to rank the stores by TotalReceipts
    , RANKINGS AS (
        SELECT      *
                    , RANK() OVER(PARTITION BY GENERATION ORDER BY TotalReceipts DESC) AS [StoreRanking]
        FROM       COUNT_STORES
    )
    --Identify the top store (i.e. where ranking = 1)
        SELECT      *
        FROM        RANKINGS
        WHERE       StoreRanking = 1
        ORDER BY    TotalReceipts
        --Walmart is the most popular for all generations

/*
GENERATION	            STORE_NAME	        TotalReceipts	   StoreRanking
The Silent Generation	    WALMART	            2	                1
Generation Z	            WALMART	            3	                1
The Baby Boomer Generation	WALMART	            14	                1
Millenials	                WALMART	            17	                1
Generation X	            WALMART	            19	                1
Unknown	                    WALMART	            10292	            1

Answer: Walmart is the most scanned store for all generations.

*/

    
--END OF SCRIPT