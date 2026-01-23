-- TABLA 1: Clientes (CRM)
CREATE TABLE silver.crm_cust_info
(
    cst_id             INT,
    cst_key            VARCHAR(50),
    cst_firstname      VARCHAR(50),
    cst_lastname       VARCHAR(50),
    cst_marital_status VARCHAR(10),
    cst_gndr           VARCHAR(50),
    cst_create_date    DATE, -- CAMBIO: De VARCHAR a DATE
    dwh_create_date    TIMESTAMP DEFAULT NOW()
);

-- TABLA 2: Productos (CRM)
DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info
(
    prd_id          INT,
    cat_id          VARCHAR(50),
    prd_key         VARCHAR(50),
    prd_nm          VARCHAR(50),
    prd_cost        NUMERIC(10, 2), -- CAMBIO: De INT a NUMERIC para guardar centavos
    prd_line        VARCHAR(20),
    prd_start_dt    DATE,           -- CAMBIO: Fecha real
    prd_end_dt      DATE,           -- CAMBIO: Fecha real
    dwh_create_date TIMESTAMP DEFAULT NOW()
);

-- TABLA 3: Ventas (CRM) - ¡La más importante!
CREATE TABLE silver.crm_sales_details
(
    sls_ord_num     VARCHAR(50),
    sls_prd_key     VARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,           -- CAMBIO
    sls_ship_dt     DATE,           -- CAMBIO
    sls_due_dt      DATE,           -- CAMBIO
    sls_sales       NUMERIC(10, 2), -- CAMBIO: Ventas totales con centavos
    sls_quantity    INT,            -- Cantidad suele ser entero, está bien
    sls_price       NUMERIC(10, 2), -- CAMBIO: Precio unitario con centavos
    dwh_create_date TIMESTAMP DEFAULT NOW()
);

-- TABLAS ERP (LEGACY)
-- Nota: En Silver solemos renombrar columnas feas (CID -> cst_id),
-- pero si sigues el tutorial, mejor mantén los nombres originales por ahora.

CREATE TABLE silver.erp_cust_az12
(
    CID             VARCHAR(50),
    BDATE           DATE, -- CAMBIO: Fecha de nacimiento real
    GEN             VARCHAR(20),
    dwh_create_date TIMESTAMP DEFAULT NOW()
);

CREATE TABLE silver.erp_loc_a101
(
    CID             VARCHAR(50),
    CNTRY           VARCHAR(50),
    dwh_create_date TIMESTAMP DEFAULT NOW()
);

CREATE TABLE silver.erp_px_cat_g1v2
(
    ID              VARCHAR(50),
    CAT             VARCHAR(50),
    SUBCAT          VARCHAR(50),
    MAINTENANCE     VARCHAR(10),
    dwh_create_date TIMESTAMP DEFAULT NOW()
);