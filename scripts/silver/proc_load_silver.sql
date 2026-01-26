/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL silver.load_silver();
===============================================================================
*/
CREATE OR REPLACE PROCEDURE silver.load_silver()
    LANGUAGE plpgsql AS
$$
DECLARE
    start_time       TIMESTAMP;
    end_time         TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time   TIMESTAMP;
BEGIN
    batch_start_time := clock_timestamp();
    RAISE NOTICE '================================================';
    RAISE NOTICE '       LOADING SILVER LAYER';
    RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE '       Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- ==============================================================================
    -- TABLA 1: CUST_INFO
    -- ==============================================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE '>> Inserting Data: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info(
        cst_id, cst_key, cst_firstname, cst_lastname,
        cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE UPPER(TRIM(cst_marital_status))
            WHEN 'M' THEN 'Married'
            WHEN 'S' THEN 'Single'
            ELSE 'n/a'
        END,
        CASE UPPER(TRIM(cst_gndr))
            WHEN 'F' THEN 'Female'
            WHEN 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        cst_create_date::DATE -- Asumiendo formato YYYY-MM-DD estándar
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1; -- Deduplicación

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Time: %', (end_time - start_time);

    -- ==============================================================================
    -- TABLA 2: PRD_INFO
    -- ==============================================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info; -- ¡Faltaba esto!

    RAISE NOTICE '>> Inserting Data: silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT
        prd_id,
        replace(substr(prd_key, 1, 5), '-', '_') as cat_id,
        substr(prd_key, 7) as prd_key,
        prd_nm,
        COALESCE(prd_cost, 0),
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'N/A'
        END,
        CAST(prd_start_dt AS DATE),
        CASE
            WHEN CAST(prd_end_dt AS DATE) < CAST(prd_start_dt AS DATE) OR prd_end_dt IS NULL THEN
                LEAD(CAST(prd_start_dt AS DATE)) OVER (PARTITION BY prd_key ORDER BY CAST(prd_start_dt AS DATE)) - 1
            ELSE
                CAST(prd_end_dt AS DATE)
        END
    FROM bronze.crm_prd_info;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Time: %', (end_time - start_time);

    -- ==============================================================================
    -- TABLA 3: SALES_DETAILS
    -- ==============================================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

    RAISE NOTICE '>> Inserting Data: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details(
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt,
        sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        -- Corrección de Fechas: Usamos TO_DATE para mayor seguridad con 'YYYYMMDD'
        CASE WHEN sls_order_dt = '0' OR LENGTH(sls_order_dt) != 8 THEN NULL
             ELSE TO_DATE(sls_order_dt, 'YYYYMMDD') END,
        CASE WHEN sls_ship_dt = '0' OR LENGTH(sls_ship_dt) != 8 THEN NULL
             ELSE TO_DATE(sls_ship_dt, 'YYYYMMDD') END,
        CASE WHEN sls_due_dt = '0' OR LENGTH(sls_due_dt) != 8 THEN NULL
             ELSE TO_DATE(sls_due_dt, 'YYYYMMDD') END,
        -- Reglas de Negocio (Sales)
        CASE
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN
                sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,
        sls_quantity,
        -- Reglas de Negocio (Price)
        CASE
            WHEN sls_price IS NULL OR sls_price = 0 THEN
                ABS(sls_sales) / NULLIF(sls_quantity, 0)
            ELSE ABS(sls_price)
        END
    FROM bronze.crm_sales_details;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Time: %', (end_time - start_time);

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE '       Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- ==============================================================================
    -- TABLA 4: ERP_CUST_AZ12
    -- ==============================================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;

    RAISE NOTICE '>> Inserting Data: silver.erp_cust_az12';
    INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
    SELECT
        CASE WHEN UPPER(cid) LIKE 'NAS%' THEN substr(cid, 4) ELSE cid END,
        CASE WHEN bdate::DATE > CURRENT_DATE THEN NULL ELSE bdate::DATE END, -- Usar CURRENT_DATE
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Time: %', (end_time - start_time);

    -- ==============================================================================
    -- TABLA 5: ERP_LOC_A101
    -- ==============================================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;

    RAISE NOTICE '>> Inserting Data: silver.erp_loc_a101';
    INSERT INTO silver.erp_loc_a101(cid, cntry)
    SELECT
        replace(cid, '-', ''),
        CASE
            WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
            WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Time: %', (end_time - start_time);

    -- ==============================================================================
    -- TABLA 6: ERP_PX_CAT_G1V2
    -- ==============================================================================
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting Data: silver.erp_px_cat_g1v2';
    INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;

    end_time := clock_timestamp();
    RAISE NOTICE '>> Load Time: %', (end_time - start_time);

    -- ==============================================================================
    -- FINALIZACIÓN
    -- ==============================================================================
    batch_end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '================================================';
    RAISE NOTICE '       SILVER LAYER LOADED SUCCESSFULLY';
    RAISE NOTICE '       Total Batch Duration: %', (batch_end_time - batch_start_time);
    RAISE NOTICE '================================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'ERROR OCCURRED: %', SQLERRM;
    -- Nota: En procedimientos PL/pgSQL, el ROLLBACK es automático si hay excepción.
    -- No es necesario escribir ROLLBACK aquí explícitamente dentro del bloque EXCEPTION.
END;
$$;
