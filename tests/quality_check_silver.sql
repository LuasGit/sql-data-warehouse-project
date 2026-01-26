/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

--Searching for duplicates in PK
SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1
    OR prd_id IS NULL;

--Cleaning the duplicates
SELECT *
FROM (SELECT *,
             row_number() over (PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
      FROM silver.crm_cust_info) t
WHERE flag_last = 1;


--Check for unwanted spaces
--Expectation: No results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


--Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;


--Check for nulls or negative numbers in cost
SELECT sls_sales
from silver.crm_sales_details
WHERE sls_sales IS NULL
   OR sls_sales <= 0;

--Check for invalidate DATES
SELECT *
FROM silver.crm_sales_details
WHERE sls_due_dt < sls_ship_dt;

--Check for invalid data, nulls, zeros or negatives
SELECT DISTINCT sls_sales,
                sls_quantity,
                sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

--Searching for data issues in gen
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

--Identifying date out of range dates
SELECT BDATE
FROM silver.erp_cust_az12
WHERE bdate::date < '1924-01-01'
   OR bdate::date > NOW();

