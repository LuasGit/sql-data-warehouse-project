--Create table
CREATE TABLE bronze.crm_cust_info
(
    cst_id             INT,         -- Sin PRIMARY KEY
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(10), -- Suficiente para 'S', 'M'
    cst_gndr           VARCHAR(50), -- Podría ser CHAR(1), pero VARCHAR está bien
    cst_create_date    VARCHAR(50)  -- Ojo con el formato fecha
);

CREATE TABLE bronze.crm_prd_info
(
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(10),
    prd_start_dt VARCHAR(50),
    prd_end_dt   VARCHAR(50)
);

CREATE TABLE bronze.crm_sales_details
(
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt VARCHAR(50),
    sls_ship_dt  VARCHAR(50),
    sls_due_dt   VARCHAR(50),
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

CREATE TABLE bronze.erp_cust_az12
(
    CID   VARCHAR(50),
    BDATE VARCHAR(50),
    GEN   VARCHAR(20)
);

CREATE TABLE bronze.erp_loc_a101
(
    CID   VARCHAR(50),
    CNTRY VARCHAR(50)
);

CREATE TABLE bronze.erp_px_cat_g1v2
(
    ID          VARCHAR(50),
    CAT         VARCHAR(50),
    SUBCAT      VARCHAR(50),
    MAINTENANCE VARCHAR(10)
);