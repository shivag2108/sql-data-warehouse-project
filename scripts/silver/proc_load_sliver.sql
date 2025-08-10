/*
==========================================================================================================
Stored Procedure: Load sliver Layer (source-> sliver)
==========================================================================================================
Script Purpose:
	This stored procedure performances the ETL(Extract, Transform and Load) process to populated the 'sliver' schema tables from the 'bronze' schema.
Additional Performed:
	- Truncate sliver tables.
	- Inserts transformed and cleaned data from Bronze into sliver tables.

Parameters:
	None.
	This stored Procedure does not accept any parameters or return any values.

Use Example:
	EXE sliver.load_sliver;
*/

CREATE OR ALTER PROCEDURE sliver.load_sliver
AS
BEGIN
    DECLARE @StartTime DATETIME, @EndTime DATETIME, @DurationMs INT;

	PRINT '====================================================================='
	PRINT ' Loading Sliver Layer'
	PRINT '======================================================================'
    -------------------------------
    -- 1. crm_cust_info load
    -------------------------------
    SET @StartTime = SYSDATETIME();
    PRINT 'Loading sliver.crm_cust_info - Cleansing customer data, removing duplicates';

    INSERT INTO sliver.crm_cust_info(
        cst_id, cst_key, cst_firstname, cst_lastname,
        cst_martial_status, cst_gndr, cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
             WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
             ELSE 'n/a'
        END,
        CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
             WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
             ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

    SET @EndTime = SYSDATETIME();
    SET @DurationMs = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
    PRINT CONCAT('Completed sliver.crm_cust_info in ', @DurationMs, ' ms');

    -------------------------------
    -- 2. crm_prd_info load
    -------------------------------
    SET @StartTime = SYSDATETIME();
    PRINT 'Loading sliver.crm_prd_info - Standardizing product keys and mapping product lines';

    INSERT INTO sliver.crm_prd_info (
        prd_id, cat_id, prd_key, prd_nm, prd_cost,
        prd_line, prd_start_dt, prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_ ') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
        prd_nm,
        ISNULL(prd_cost, 0),
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        CAST(prd_start_dt AS DATE),
        CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)
    FROM bronze.crm_prd_info;

    SET @EndTime = SYSDATETIME();
    SET @DurationMs = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
    PRINT CONCAT('Completed sliver.crm_prd_info in ', @DurationMs, ' ms');

    -------------------------------
    -- 3. crm_sales_details load
    -------------------------------
    SET @StartTime = SYSDATETIME();
    PRINT 'Loading sliver.crm_sales_details - Converting dates and recalculating sales if mismatched';

    INSERT INTO sliver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id,
        sls_order_dt, sls_ship_dt, sls_due_dt,
        sls_sales, sls_quantity, sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END,
        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END,
        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
             ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END,
        CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
             THEN sls_quantity * ABS(sls_price)
             ELSE sls_sales
        END,
        sls_quantity,
        CASE WHEN sls_price IS NULL OR sls_price <= 0
             THEN sls_sales / NULLIF(sls_quantity, 0)
             ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    SET @EndTime = SYSDATETIME();
    SET @DurationMs = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
    PRINT CONCAT('Completed sliver.crm_sales_details in ', @DurationMs, ' ms');

    -------------------------------
    -- 4. erp_cust_az12 load
    -------------------------------
    SET @StartTime = SYSDATETIME();
    PRINT 'Loading sliver.erp_cust_az12 - Standardizing customer IDs and genders';

    INSERT INTO sliver.erp_cust_az12 (
        cid, bdate, gen
    )
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
             ELSE cid
        END,
        CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
        CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
             WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
             ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;

    SET @EndTime = SYSDATETIME();
    SET @DurationMs = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
    PRINT CONCAT('Completed sliver.erp_cust_az12 in ', @DurationMs, ' ms');

    -------------------------------
    -- 5. erp_loc_a101 load
    -------------------------------
    SET @StartTime = SYSDATETIME();
    PRINT 'Loading sliver.erp_loc_a101 - Standardizing country codes';

    INSERT INTO sliver.erp_loc_a101 (
        cid, cntry
    )
    SELECT
        REPLACE(cid, '-', ''),
        CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
             WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
             WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
             ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    SET @EndTime = SYSDATETIME();
    SET @DurationMs = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
    PRINT CONCAT('Completed sliver.erp_loc_a101 in ', @DurationMs, ' ms');

    -------------------------------
    -- 6. erp_px_cat_g1v2 load
    -------------------------------
    SET @StartTime = SYSDATETIME();
    PRINT 'Loading sliver.erp_px_cat_g1v2 - Direct copy of product category data';

    INSERT INTO sliver.erp_px_cat_g1v2 (
        id, cat, subcat, maintenance
    )
    SELECT id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;

    SET @EndTime = SYSDATETIME();
    SET @DurationMs = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
    PRINT CONCAT('Completed sliver.erp_px_cat_g1v2 in ', @DurationMs, ' ms');

END;





