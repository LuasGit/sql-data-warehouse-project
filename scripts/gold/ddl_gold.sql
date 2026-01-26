/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse.
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
CREATE OR REPLACE VIEW gold.dim_customers AS
SELECT row_number() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate Key
       xci.cst_id                          AS customer_id,
       xci.cst_key                         AS customer_number,
       xci.cst_firstname                   AS first_name,
       xci.cst_lastname                    AS last_name,
       xlo.cntry                           AS country,
       xci.cst_marital_status              AS marital_status,
       CASE
           WHEN xci.cst_gndr != 'n/a' THEN xci.cst_gndr
           ELSE COALESCE(xca.gen, 'n/a')
       END                                 AS gender,
       xca.bdate                           AS birthday,
       xci.cst_create_date                 AS create_date
FROM silver.crm_cust_info xci
     LEFT JOIN silver.erp_cust_az12 xca ON xci.cst_key = xca.cid
     LEFT JOIN silver.erp_loc_a101 xlo ON xci.cst_key = xlo.cid;

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
CREATE OR REPLACE VIEW gold.dim_products AS
SELECT ROW_NUMBER() OVER (ORDER BY prd_start_dt, prd_key) AS product_key, -- Surrogate Key
       xprd.prd_id                                        AS product_id,
       xprd.prd_key                                       AS product_number,
       xprd.prd_nm                                        AS product_name,
       xprd.cat_id                                        AS category_id,
       xcat.cat                                           AS category,
       xcat.subcat                                        AS subcategory,
       xcat.maintenance,
       xprd.prd_cost                                      AS cost,
       xprd.prd_line                                      AS line,
       xprd.prd_start_dt                                  AS start_date
FROM silver.crm_prd_info xprd
     LEFT JOIN silver.erp_px_cat_g1v2 xcat ON xprd.cat_id = xcat.id
WHERE xprd.prd_end_dt IS NULL; -- Solo productos activos actual

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT xsls.sls_ord_num  AS order_name,
       xprd.product_key,
       xcus.customer_key,
       xsls.sls_order_dt AS order_date,
       xsls.sls_ship_dt  AS shipping_date,
       xsls.sls_due_dt   AS due_date,
       xsls.sls_sales    AS sales_amount,
       xsls.sls_quantity AS quantity,
       xsls.sls_price    AS price
FROM silver.crm_sales_details xsls
     LEFT JOIN gold.dim_customers xcus ON xcus.customer_id = xsls.sls_cust_id
     LEFT JOIN gold.dim_products xprd ON xprd.product_number = xsls.sls_prd_key;