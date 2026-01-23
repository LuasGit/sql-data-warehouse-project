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
      FROM bronze.crm_cust_info) t
WHERE flag_last = 1;


--Check for unwanted spaces
--Expectation: No results
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


--Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

--Making F = Female, M = Male, Null= n/a, No abbreviations in our DWH
SELECT CASE cst_gndr
           WHEN 'F' THEN 'Female'
           WHEN 'M' THEN 'Male'
           ELSE 'n/a'
           END
FROM bronze.crm_cust_info;




--Check for nulls or negative numbers in cost
SELECT prd_cost
from silver.crm_prd_info
WHERE prd_cost IS NULL
   OR prd_cost < 0;

--Check for invalidate DATES
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT prd_id,
       prd_key,
       replace(substr(prd_key, 1, 5), '-', '_') as cat_id,
       substr(prd_key, 7)                       as prd_key_clean, -- Optimizado
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
