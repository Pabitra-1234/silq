select * from orders
select * from products
select * from users


-- Data handling(combined all the datas together using cte)

WITH cte AS (
    SELECT o.OrderID, o.UserID, o.ProductID, o.OrderDate, o.Amount, p.ProductName, p.Price, u.SignUpDate, u.Location
    FROM orders o
    INNER JOIN Users u ON o.userid = u.userid
    INNER JOIN Products AS p ON o.productid = p.productid
    WHERE o.orderdate IS NOT NULL AND u.signupdate IS NOT NULL -- Validation for null dates
)
SELECT * INTO retail FROM cte;
select * from retail

----Cohort_Analysis-----------------

-- Create a cohort based on the first purchase date (orderDate)

WITH first_purchases AS (
    SELECT userid, MIN(orderDate) AS first_purchase_date
    FROM retail
    GROUP BY userid
)
-- Select user IDs, their first purchase date, and cohort date (truncated to month)
SELECT 
    userid,
    first_purchase_date,
    DATE_TRUNC('month', first_purchase_date) AS Cohort_Date  -- Use DATE_TRUNC for month extraction
INTO cohort
FROM first_purchases;

-- Display the cohort table
SELECT * FROM cohort;

----------------- Create Cohort Index (Integer representation of the number of months passed since the customer's first purchase) -------------------------
-- Display the retail table
SELECT * FROM retail;

-- Join retail data with cohort information and calculate cohort indices
SELECT
    m.*,
    c.Cohort_Date,
    EXTRACT(YEAR FROM m.orderdate) AS order_year,
    EXTRACT(MONTH FROM m.orderdate) AS order_month,
    EXTRACT(YEAR FROM c.Cohort_Date) AS cohort_year,
    EXTRACT(MONTH FROM c.Cohort_Date) AS cohort_month,
    (EXTRACT(YEAR FROM m.orderdate) - EXTRACT(YEAR FROM c.Cohort_Date)) AS year_diff,
    (EXTRACT(MONTH FROM m.orderdate) - EXTRACT(MONTH FROM c.Cohort_Date)) AS month_diff,
    (EXTRACT(YEAR FROM m.orderdate) - EXTRACT(YEAR FROM c.Cohort_Date)) * 12 +
    (EXTRACT(MONTH FROM m.orderdate) - EXTRACT(MONTH FROM c.Cohort_Date)) + 1 AS cohort_index
INTO cohort_retention
FROM retail m
LEFT JOIN cohort c ON m.userid = c.userid;

-- Display the cohort retention table
SELECT * FROM cohort_retention;

--- Pivot Data to see the cohort table------
-- Create a table with distinct customers, cohort dates, and cohort indices
WITH distinct_customers AS (
    SELECT DISTINCT userID, Cohort_Date, cohort_index
    FROM cohort_retention
)
-- Pivot the data to get cohort-wise user counts
SELECT  
    Cohort_Date,
    COUNT(CASE WHEN cohort_index = 1 THEN userid END) AS "Cohort 1",
    COUNT(CASE WHEN cohort_index = 2 THEN userid END) AS "Cohort 2",
    COUNT(CASE WHEN cohort_index = 3 THEN userid END) AS "Cohort 3",
    COUNT(CASE WHEN cohort_index = 4 THEN userid END) AS "Cohort 4",
    COUNT(CASE WHEN cohort_index = 5 THEN userid END) AS "Cohort 5",
    COUNT(CASE WHEN cohort_index = 6 THEN userid END) AS "Cohort 6",
    COUNT(CASE WHEN cohort_index = 7 THEN userid END) AS "Cohort 7",
    COUNT(CASE WHEN cohort_index = 8 THEN userid END) AS "Cohort 8",
    COUNT(CASE WHEN cohort_index = 9 THEN userid END) AS "Cohort 9",
    COUNT(CASE WHEN cohort_index = 10 THEN userid END) AS "Cohort 10",
    COUNT(CASE WHEN cohort_index = 11 THEN userid END) AS "Cohort 11",
    COUNT(CASE WHEN cohort_index = 12 THEN userid END) AS "Cohort 12",
    COUNT(CASE WHEN cohort_index = 13 THEN userid END) AS "Cohort 13"
INTO cohort_pivot
FROM distinct_customers
GROUP BY Cohort_Date;

-- Display the cohort pivot table
SELECT * FROM cohort_pivot
ORDER BY Cohort_Date;


---TASK-2

------Group users into monthly cohorts based on their sign-up date.
 
-- Create a cohort based on the first buy date (signupdate)
WITH cohort1 AS (
    SELECT 
        userid,
        MIN(signupdate) AS first_buy_date,
        DATE_TRUNC('month', MIN(signupdate)) AS Cohort_Date1
    FROM retail
    GROUP BY userid
)
-- Join retail data with cohort information and calculate cohort indices
SELECT
    r.*,
    c.Cohort_Date1,
    EXTRACT(YEAR FROM r.signupdate) AS signup_year1,
    EXTRACT(MONTH FROM r.signupdate) AS signup_month1,
    EXTRACT(YEAR FROM c.Cohort_Date1) AS cohort_year1,
    EXTRACT(MONTH FROM c.Cohort_Date1) AS cohort_month1,
    EXTRACT('year' FROM AGE(r.signupdate, c.Cohort_Date1)) AS year_diff1,
    EXTRACT('month' FROM AGE(r.signupdate, c.Cohort_Date1)) AS month_diff1,
    EXTRACT('year' FROM AGE(r.signupdate, c.Cohort_Date1)) * 12 +
    EXTRACT('month' FROM AGE(r.signupdate, c.Cohort_Date1)) + 1 AS cohort_index1
INTO cohort_retention1
FROM retail r
LEFT JOIN cohort1 c ON r.userid = c.userid;

-- Create a table with distinct customers, cohort dates, and cohort indices
WITH distinct_customers1 AS (
    SELECT DISTINCT userID, Cohort_Date1, cohort_index1
    FROM cohort_retention1
)
-- Pivot the data to get cohort-wise user counts
SELECT 
    Cohort_Date1,
    COUNT(CASE WHEN cohort_index1 = 1 THEN userid END) AS "Cohort 1",
    COUNT(CASE WHEN cohort_index1 = 2 THEN userid END) AS "Cohort 2",
    COUNT(CASE WHEN cohort_index1 = 3 THEN userid END) AS "Cohort 3",
    COUNT(CASE WHEN cohort_index1 = 4 THEN userid END) AS "Cohort 4",
    COUNT(CASE WHEN cohort_index1 = 5 THEN userid END) AS "Cohort 5",
    COUNT(CASE WHEN cohort_index1 = 6 THEN userid END) AS "Cohort 6",
    COUNT(CASE WHEN cohort_index1 = 7 THEN userid END) AS "Cohort 7",
    COUNT(CASE WHEN cohort_index1 = 8 THEN userid END) AS "Cohort 8",
    COUNT(CASE WHEN cohort_index1 = 9 THEN userid END) AS "Cohort 9",
    COUNT(CASE WHEN cohort_index1 = 10 THEN userid END) AS "Cohort 10",
    COUNT(CASE WHEN cohort_index1 = 11 THEN userid END) AS "Cohort 11",
    COUNT(CASE WHEN cohort_index1 = 12 THEN userid END) AS "Cohort 12",
    COUNT(CASE WHEN cohort_index1 = 13 THEN userid END) AS "Cohort 13"
INTO cohort_pivot1
FROM distinct_customers1
GROUP BY Cohort_Date1
ORDER BY Cohort_Date1;

-- Display the result
SELECT * FROM cohort_pivot1
ORDER BY 1;


----------------------------------------------------------------------------------

-----TASK-3
---Calculate the retention rate for each cohort over a 3-month period.

  -- Select relevant columns from retail table, joining with cohort1 to get cohort-related information

SELECT
    r.*,
    c.Cohort_Date1,
    EXTRACT(YEAR FROM r.signupdate) AS signup_year1,
    EXTRACT(MONTH FROM r.signupdate) AS signup_month1,
    EXTRACT(YEAR FROM c.Cohort_Date1) AS cohort_year1,
    EXTRACT(MONTH FROM c.Cohort_Date1) AS cohort_month1,
    AGE(r.signupdate, c.Cohort_Date1) AS cohort_age,
    EXTRACT('year' FROM AGE(r.SignUpDate, c.Cohort_Date1)) * 12 +
    EXTRACT('month' FROM AGE(r.SignUpDate, c.Cohort_Date1)) + 1 AS cohort_index
INTO cohort_retention4
FROM retail r
LEFT JOIN cohort1 c ON r.userid = c.userid;

-- Display the cohort_retention4 table
SELECT * FROM cohort_retention4;

-- Create a retention_filter table by selecting relevant columns from cohort_retention1 based on a cohort_index1 filter
SELECT * INTO retention_filter
FROM cohort_retention4
WHERE cohort_index <= 3; -- Filter for the first 3 periods

-- Display the retention_filter table
SELECT * FROM retention_filter;

-- Calculate retention metrics for each cohort over a 3-month period and store the results in the retention_rate table
SELECT
    Cohort_Date1,
    COUNT(DISTINCT CASE WHEN cohort_index = 1 THEN userid END) AS "Initial Cohort Size",
    COUNT(DISTINCT CASE WHEN cohort_index = 1 THEN userid END) AS "Month 1 Retention",
    COUNT(DISTINCT CASE WHEN cohort_index = 2 THEN userid END) AS "Month 2 Retention",
    COUNT(DISTINCT CASE WHEN cohort_index = 3 THEN userid END) AS "Month 3 Retention",
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN cohort_index = 1 THEN userid END) /
        COUNT(DISTINCT CASE WHEN cohort_index = 1 THEN userid END),
        2
    ) AS "Month 1 Retention Rate (%)",
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN cohort_index = 2 THEN userid END) /
        COUNT(DISTINCT CASE WHEN cohort_index = 1 THEN userid END),
        2
    ) AS "Month 2 Retention Rate (%)",
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN cohort_index = 3 THEN userid END) /
        COUNT(DISTINCT CASE WHEN cohort_index = 1 THEN userid END),
        2
    ) AS "Month 3 Retention Rate (%)"
INTO retention_rate
FROM retention_filter
GROUP BY Cohort_Date1
ORDER BY Cohort_Date1;

-- Display the retention_rate table
SELECT * FROM retention_rate;





