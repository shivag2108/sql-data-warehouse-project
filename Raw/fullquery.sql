USE master;


--Create Database 'DataWarehouse'

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

-- Create Schemas
 
CREATE SCHEMA bronze;

CREATE SCHEMA sliver;

CREATE SCHEMA gold;


------------------------------------------------------------------
-- Create SQL DDL scripts for all CSV files

--IF OBJECT_ID ('bronze.crm_cust_info' , 'U') IS NOT NULL
--	DROP TABLE bronze.crm_cust_info; ---wrute at each table.

-- CRM files:-

CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR (50),
	cst_firstname NVARCHAR (50),
	cst_lastname NVARCHAR (50),
	cst_martial_status NVARCHAR (50),
	cst_gndr NVARCHAR (50),
	cst_create_date DATE
);

-- EXEC SP_rename 'bronze.crm_cust_info.cst_martial_status', 'cst_material_status';

CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR (50),
	prd_nm NVARCHAR (50),
	prd_cost INT,
	prd_line NVARCHAR (50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);

CREATE TABLE bronze.crm_sales_details (
	sls_ord_num NVARCHAR (50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
DROP TABLE bronze.crm_sales_details

-- ERP files

CREATE TABLE bronze.erp_loc_a101 (
	cid NVARCHAR (50),
	cntry NVARCHAR (50)
);

CREATE TABLE bronze.erp_cust_az12 (
	cid NVARCHAR (50),
	bdate DATE,
	gen NVARCHAR (50)
);

CREATE TABLE bronze.erp_px_cat_g1v2 (
	id NVARCHAR (50),
	cat NVARCHAR (50),
	subcat NVARCHAR (50),
	maintenance NVARCHAR (50)
);

-- BULK INSERT(crm files):-
-- Before load truncate the table to avoid duplicates. every time you run it gives same input

TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\sivak\OneDrive\Desktop\Data Engineer\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
);
SELECT * FROM bronze.crm_cust_info;   --- This is called fullload.

----------

TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\sivak\OneDrive\Desktop\Data Engineer\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
);
SELECT * FROM bronze.crm_prd_info;

-------------

TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\sivak\OneDrive\Desktop\Data Engineer\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
);
SELECT * FROM bronze.crm_sales_details;

--- BULK INSERT(erp files)

TRUNCATE TABLE bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\sivak\OneDrive\Desktop\Data Engineer\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
);
SELECT * FROM bronze.erp_cust_az12;

----------

TRUNCATE TABLE bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\sivak\OneDrive\Desktop\Data Engineer\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
);
SELECT * FROM bronze.erp_loc_a101;

----------

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\sivak\OneDrive\Desktop\Data Engineer\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.CSV'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK 
);
SELECT * FROM bronze.erp_px_cat_g1v2;

----------------------------------------------------------------------------------------
---- Create stored procedure for bronze for easy query.
----------------------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=================================================';

		PRINT '-------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT ' Insert Data Into: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\sivak\OneDrive\Desktop\Data Engineer\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>>----------------';

		SET @start_time = GETDATE();
		PRINT ' Insert Data Into: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\sivak\OneDrive\Desktop\Data Engineer\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>>----------------';

		SET @start_time = GETDATE();
		PRINT ' Insert Data Into: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\sivak\OneDrive\Desktop\Data Engineer\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>>----------------';

		PRINT '-------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT ' Insert Data Into: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\sivak\OneDrive\Desktop\Data Engineer\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>>----------------';

		SET @start_time = GETDATE();
		PRINT ' Insert Data Into: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\sivak\OneDrive\Desktop\Data Engineer\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>>----------------';

		SET @start_time = GETDATE();
		PRINT ' Insert Data Into: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\sivak\OneDrive\Desktop\Data Engineer\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' Seconds';
		PRINT '>>----------------';

		SET @batch_end_time = GETDATE();
		PRINT '========================================================'
		PRINT 'Loading Bronze Layer Completed';
		PRINT ' - Total Load Duration' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' Seconds';
		PRINT '========================================================'

	END TRY
	BEGIN CATCH
		PRINT '================================================='
		PRINT 'ERROR OCCURDED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '================================================='
	END CATCH
END

EXEC bronze.load_bronze;

/*=====================================================================================

		SLIVER LAYER

 ========================================================================================*/

 -- Create DDL scripts for Slive Layer:-

 
CREATE TABLE sliver.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR (50),
	cst_firstname NVARCHAR (50),
	cst_lastname NVARCHAR (50),
	cst_martial_status NVARCHAR (50),
	cst_gndr NVARCHAR (50),
	cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

CREATE TABLE sliver.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR (50),
	prd_nm NVARCHAR (50),
	prd_cost INT,
	prd_line NVARCHAR (50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

GO

CREATE TABLE sliver.crm_sales_details (
	sls_ord_num NVARCHAR (50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

GO
-- ERP files

CREATE TABLE sliver.erp_loc_a101 (
	cid NVARCHAR (50),
	cntry NVARCHAR (50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

GO

CREATE TABLE sliver.erp_cust_az12 (
	cid NVARCHAR (50),
	bdate DATE,
	gen NVARCHAR (50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

CREATE TABLE sliver.erp_px_cat_g1v2 (
	id NVARCHAR (50),
	cat NVARCHAR (50),
	subcat NVARCHAR (50),
	maintenance NVARCHAR (50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

/* Here we need to clean all data in bronze layer and insert into sliver layer
	so it has cleaned data.
*/
SELECT *
FROM bronze.crm_cust_info -- check for nulls or duplicates in primary key.Exception: no result.
  
SELECT 
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;--- Here 5 records are duplicates and one record is null.

SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

SELECT *
FROM
(
	SELECT *,
		ROW_NUMBER () OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id = 29466
	) t
	WHERE flag_last =1 AND cst_id = 29466;

-- Check unwanted spaces in string values:- no result

SELECT 
	cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT 
	cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT 
	cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);-- no spaces so it is good.

-- Data Standardization & Consistency

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

SELECT DISTINCT cst_material_status
FROM bronze.crm_cust_info


-- After check duplicates in primary column, spaces and inconsistency in string columns so we insert data into the sliver layer.

----------------------------------- INSERT INTO data from bronze to sliver -----------------------------------
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

SELECT *
FROM sliver.crm_cust_info;

EXEC sp_help 'sliver.crm_cust_info'
------------------------- clean & load cmr_prd_info to Sliver ----------------------

SELECT * 
FROM bronze.crm_prd_info;

SELECT * 
FROM sliver.crm_prd_info; -- after insert to sliver

--- Check is there duplicates & null in primary key:- no result

SELECT 
	prd_id,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT 
	prd_id,
	COUNT(*)
FROM sliver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check unwanted spaces:- No result

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM (prd_nm);

SELECT prd_nm
FROM sliver.crm_prd_info
WHERE prd_nm != TRIM (prd_nm);


-- Check for nulls or negative numbers

SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

SELECT prd_cost
FROM sliver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data standardization & consistency

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

SELECT DISTINCT prd_line
FROM sliver.crm_prd_info;


-- Check for Invalid dates

SELECT * 
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt; -- end date must not be earlier than the start date.

SELECT * 
FROM sliver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

-- There are two solutions to solve Invalid dates:
-- 1. Switch end data and start date. But, in this there is an overlapping issue
-- 2. Derive all end dates from start dates. That means 
-- End date = Start date of next record - 1.

SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info


------ to check two tables rows match or not -------------------
SELECT
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_ ') AS cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_ ') NOT IN (select DISTINCT id from bronze.erp_px_cat_g1v2)

-------------- insert into sliver.crm_prd_info -----------------------------

select * from bronze.crm_prd_info
SELECT * FROM sliver.crm_prd_info -- In this we want to add cat_id so we drop this table create new table with added column

IF OBJECT_ID('sliver.crm_prd_info', 'U') IS NOT NULL 
	DROP TABLE sliver.crm_prd_info;
CREATE TABLE sliver.crm_prd_info (
	prd_id INT,
	cat_id NVARCHAR (50),
	prd_key NVARCHAR (50),
	prd_nm NVARCHAR (50),
	prd_cost INT,
	prd_line NVARCHAR (50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

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
	REPLACE(SUBSTRING(RTRIM(LTRIM(prd_key)), 1, 5), '-', '_') AS cat_id, -- Extra category ID
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

SELECT len(cat_id) FROM sliver.crm_prd_info;
select * from bronze.crm_prd_info
-------------------------------------- Clean & Load crm_sales_details into Sliver -----------------------------------------

-- Check spaces:-

SELECT *
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Check for invalid dates(here dates in integer):-

SELECT 
	sls_order_dt
FROM bronze.crm_sales_details
Where sls_order_dt <=0;    -- Negative no. or zeros can't be cast to a date. then we replace that values with nulls

SELECT 
	NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
Where sls_order_dt <=0;

-- Here length of the integer date must be length-8(20201229) or else it is not date.

SELECT 
NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
Where sls_order_dt <=0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 2050010
OR sls_order_dt < 19000101; -- It defines max data acquire date min to max

-- Same as ship_date and due_date
-- Order_date/ start_date always smaller than due_date, Ship_date and end_date.

SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


-- check sales prd_key with products prd_key

SELECT  
	sls_ord_num ,
	sls_prd_key ,
	sls_cust_id ,
	sls_order_dt ,
	sls_ship_dt ,
	sls_due_dt ,
	sls_sales ,
	sls_quantity ,
	sls_price 
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN ( SELECT prd_key FROM sliver.crm_prd_info);

-- check sales sla_cust_id with products cst_id

SELECT 
	sls_ord_num ,
	sls_prd_key ,
	sls_cust_id ,
	sls_order_dt ,
	sls_ship_dt ,
	sls_due_dt ,
	sls_sales ,
	sls_quantity ,
	sls_price 
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN ( SELECT cst_id FROM sliver.crm_cust_info);

-----------------

-- Check data consistency: between sales, quantity and price
-->> Sales = Quantity * Price
-->> values must not be NULL, zero or negative.

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

--final

SELECT DISTINCT
sls_sales AS old_sales,
sls_quantity,
sls_price AS old_price,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales/ NULLIF(sls_quantity, 0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


--- Insert data into sliver from bronze -----------


IF OBJECT_ID('sliver.crm_sales_details','U') IS NOT NULL
	DROP TABLE sliver.crm_sales_details;
CREATE TABLE sliver.crm_sales_details (
	sls_ord_num NVARCHAR (50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE, --- change datatype int to date so we drop previous table and create new table.
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);


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

SELECT * FROM sliver.crm_sales_details; -- recheck data with replace bronze too sliver

---------------------------- Clean & Load erp_cust_az12 ---------------------------

SELECT * 
FROM bronze.erp_cust_az12;

SELECT * 
FROM [sliver].[crm_cust_info];

-- use case to remove NAS from cid

-- Identify out-of-range date (bdate)

SELECT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1925-01-01' OR bdate > GETDATE(); 

-- Report to source system or leave it or clean data(ADD NULL).

-- Data standardization & consistency:

SELECT DISTINCT 
	gen,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
		 ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12



----------------- Insert data from bronze to sliver --------------------------------------------

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

select cid, count(*)
FROM sliver.erp_cust_az12
group by cid
having count(*) >1;
----------------------------------- Clean & Load erp_loc_a101 to sliver ---------------------------------

SELECT * 
FROM bronze.erp_loc_a101;

SELECT * 
FROM bronze.crm_cust_info;

-- Handle invalid values

SELECT 
	REPLACE(cid, '-', ''),
	cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM sliver.crm_cust_info);

-- Data standardization & consistency:

SELECT DISTINCT cntry AS Old_cntry,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = ' ' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;


---------------------------- INSERT INTO data from bronze to sliver --------------------------------

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

SELECT * FROM sliver.erp_loc_a101;

----------------------------Clean & Load erp_px_cat_g11v2 ------------------------

SELECT * FROM bronze.erp_px_cat_g1v2
SELECT * FROM sliver.crm_prd_info

-- Check unwanted spaces:

SELECT *
from bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardizationn & Consistency:

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;


SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;

----------------------------- INSERT data into sliver from bronze ------------------------------

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

SELECT * FROM sliver.erp_px_cat_g1v2

/* ==============================================================================================================================

	STORED IN PROCEDURE NAME sliver.load_sliver
*/
EXEC sliver.load_sliver

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
GO
/* =======================================================================
=============================================================================================
				GOLD LAYER
======================================================================================
=============================================================================================*/

-- Create customers table for business: (view)

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, --- create surrogate key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_martial_status AS martial_status,
	CASE WHEN cst_gndr != 'n/a' THEN cst_gndr --- CRM is the masterr for gender.
			 ELSE COALESCE(ca.gen, 'n/a')
		END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM sliver.crm_cust_info ci
LEFT JOIN sliver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN sliver.erp_loc_a101 AS la
ON ci.cst_key = la.cid

SELECT * FROM gold.dim_customers

-- After joining we need to check is there any duplicates after join logic

SELECT cst_id, count(*)
FROM 
(
	SELECT 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_martial_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM sliver.crm_cust_info ci
	LEFT JOIN sliver.erp_cust_az12 AS ca
	ON ci.cst_key = ca.cid
	LEFT JOIN sliver.erp_loc_a101 AS la
	ON ci.cst_key = la.cid
) t 
GROUP BY cst_id
hAVING COUNT(*) > 1;

---------

SELECT DISTINCT
		ci.cst_gndr,
		ca.gen,
		CASE WHEN cst_gndr != 'n/a' THEN cst_gndr
			 ELSE COALESCE(ca.gen, 'n/a')
		END AS new_gen
	FROM sliver.crm_cust_info ci
	LEFT JOIN sliver.erp_cust_az12 AS ca
	ON ci.cst_key = ca.cid
	LEFT JOIN sliver.erp_loc_a101 AS la
	ON ci.cst_key = la.cid
	ORDER BY 1, 2



-- Create view for Products and Business logic:

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER( ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat category,
	pc.subcat subcategory,
	pc.maintenance,
	pn.prd_cost AS product_cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM sliver.crm_prd_info AS pn
LEFT JOIN sliver.erp_px_cat_g1v2 AS pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL

SELECT * FROM gold.dim_products

--- Quality check:

select prd_key, COUNT(*)
FROM(
SELECT 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM sliver.crm_prd_info AS pn
LEFT JOIN sliver.erp_px_cat_g1v2 AS pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL -- Filter all current data no historical data
)T
GROUP BY prd_key
HAVING COUNT(*) > 1;

--- Create View for sales and Business logic:(FACT TABLE)

CREATE VIEW gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key, -- surrogate key
	cu.customer_key, -- surrogatte key
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM sliver.crm_sales_details sd
LEFT JOIN gold.dim_products AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id

-- Foreign key integrity (dimensions)

SELECT * 
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON c.customer_key = f.customer_key 
LEFT JOIN gold.dim_products as p
ON p.product_key = f.product_key
WHERE C.customer_key IS NULL







