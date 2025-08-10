/*
==========================================================================================================
Stored Procedure: Load Bronze Layer (source-> Bronze)
==========================================================================================================
Script Purpose:
  This Procedure loads data into the 'bronze' schema from external csv files.
    - Truncate the bronze tables before loading data.
    - Uses 'BULK INSERT' command to load data from csv files to bronze tables.
*/

INSERT INTO sliver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_martial_status,
		cst_gndr,
		cst_create_date)
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
		  WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
		  ELSE 'n/a'
	END cst_material_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		  WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		  ELSE 'n/a'
	END cst_gndr,
	cst_create_date
FROM (
		SELECT 
		*,
		ROW_NUMBER () OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1;


INSERT INTO sliver.crm_prd_info (
	prd_id ,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt 
)
	
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_ ') AS cat_id, -- Extra category ID
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extra product key
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE UPPER(TRIM(prd_line))
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,	-- Map product line values into descriptive values
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 
		AS date
		) AS prd_end_dt -- Calculate end date as one day before the next start date
FROM bronze.crm_prd_info;


INSERT INTO sliver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt, --- change datatype int to date so we drop previous table and create new table.
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT 
	sls_ord_num ,
	sls_prd_key ,
	sls_cust_id ,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THen NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THen NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THen NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales ,
	sls_quantity ,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales/ NULLIF(sls_quantity, 0)
	ELSE sls_price
END AS sls_price 
FROM bronze.crm_sales_details;


INSERT INTO sliver.erp_cust_az12(
	cid,
	bdate,
	gen
)
SELECT 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		 ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL 
		 ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
		 ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;


INSERT INTO sliver.erp_loc_a101(
	cid,
	cntry
)
SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;

INSERT INTO sliver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
)
SELECT 
	id, 
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2;






