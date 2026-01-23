--Cleaning Query and Inserting

--Inserting crm_cust_info
INSERT INTO silver.crm_cust_info(cst_id,
                                 cst_key,
                                 cst_firstname,
                                 cst_lastname,
                                 cst_marital_status,
                                 cst_gndr,
                                 cst_create_date)
SELECT cst_id,
       cst_key,
       TRIM(cst_firstname) as cst_firstname,
       TRIM(cst_lastname)  as cst_lastname,
       CASE UPPER(TRIM(cst_marital_status))
           WHEN 'M' THEN 'Married'
           WHEN 'S' THEN 'Single'
           ELSE 'n/a'
           END             as cst_marital_status,
       CASE UPPER(TRIM(cst_gndr))
           WHEN 'F' THEN 'Female'
           WHEN 'M' THEN 'Male'
           ELSE 'n/a'
           END             as cst_gndr,
       cst_create_date::DATE
FROM (SELECT *,
             row_number() over (PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
      FROM bronze.crm_cust_info) t
WHERE flag_last = 1
  AND cst_id IS NOT NULL;

--Inserting crm_cust_info
INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
SELECT prd_id,
       replace(substr(prd_key, 1, 5), '-', '_') as cat_id,
       substr(prd_key, 7)                       as prd_key, -- Optimizado
       prd_nm,
       COALESCE(prd_cost, 0)                    as prd_cost,
       CASE UPPER(TRIM(prd_line))
           WHEN 'M' THEN 'Mountain'
           WHEN 'R' THEN 'Road'
           WHEN 'S' THEN 'Other Sales'
           WHEN 'T' THEN 'Touring'
           ELSE 'N/A'
           END                                  as prd_line,
       CAST(prd_start_dt AS DATE)               as prd_start_dt,

       CASE
           -- Si la fecha fin es menor a la inicio (Casteadas!), o es Nula...
           WHEN CAST(prd_end_dt AS DATE) < CAST(prd_start_dt AS DATE) OR prd_end_dt IS NULL THEN
               -- ...entonces calculamos el fin basado en el siguiente inicio
                       LEAD(CAST(prd_start_dt AS DATE))
                       OVER (PARTITION BY prd_key ORDER BY CAST(prd_start_dt AS DATE)) - 1
           ELSE
               -- ...si no, respetamos la fecha original
               CAST(prd_end_dt AS DATE)
           END                                  AS prd_end_dt_final

FROM bronze.crm_prd_info;

SELECT * FROM silver.crm_prd_info;
