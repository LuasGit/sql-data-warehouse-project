/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.

Parameters:
    None.
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
 */
create or replace procedure load_bronze()
    language plpgsql
as
$$
DECLARE
    -- 1. Declaramos variables ANTES del BEGIN
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    -- 2. Inicio del Bloque Lógico
    batch_start_time := CLOCK_TIMESTAMP(); -- Usamos := para asignar

    RAISE NOTICE '=====================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '=====================';

    RAISE NOTICE '=====================';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '=====================';

    -- TABLA 1: CUST_INFO
    start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE '>> Inserting Data: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM 'C:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );

    end_time := CLOCK_TIMESTAMP();
    -- 3. Cálculo de tiempo usando age() o resta simple y concatenación con ||
    RAISE NOTICE 'Load Time: %', (end_time - start_time);

    -- TABLA 2: PRD_INFO
    start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE '>> Inserting Data: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM 'C:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );
    end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Load Time: %', (end_time - start_time);

    -- TABLA 3: SALES_DETAILS
    start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE '>> Inserting Data: bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM 'C:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );
    end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Load Time: %', (end_time - start_time);

    RAISE NOTICE '=====================';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '=====================';

    -- TABLA 4: LOC_A101
    start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    COPY bronze.erp_loc_a101
    FROM 'C:\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );
    end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Load Time: %', (end_time - start_time);

    -- TABLA 5: CUST_AZ12
    start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    COPY bronze.erp_cust_az12
    FROM 'C:\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );
    end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Load Time: %', (end_time - start_time);

    -- TABLA 6: PX_CAT_G1V2
    start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    COPY bronze.erp_px_cat_g1v2
    FROM 'C:\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (
        FORMAT csv,
        HEADER true,
        DELIMITER ','
    );
    end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Load Time: %', (end_time - start_time);

    -- Fin del proceso
    batch_end_time := CLOCK_TIMESTAMP();
    RAISE NOTICE '-----------------------';
    RAISE NOTICE 'Load Time Duration Batch: %', (batch_end_time - batch_start_time);
    RAISE NOTICE '-----------------------';

-- 4. Manejo de Errores
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'ERROR OCCURRED: %', SQLERRM;
    ROLLBACK;
END;
$$;


