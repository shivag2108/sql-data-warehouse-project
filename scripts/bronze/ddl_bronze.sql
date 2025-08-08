-- Create SQL DDL scripts for all CSV files in Bronze schema

IF OBJECT_ID ('bronze.crm_cust_info' , 'U') IS NOT NULL
DROP TABLE bronze.crm_cust_info; ---wrute at each table.

  -- CRM files:-
GO

CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR (50),
	cst_firstname NVARCHAR (50),
	cst_lastname NVARCHAR (50),
	cst_martial_status NVARCHAR (50),
	cst_gndr NVARCHAR (50),
	cst_create_date DATE
);

GO
  
CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR (50),
	prd_nm NVARCHAR (50),
	prd_cost INT,
	prd_line NVARCHAR (50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);

GO

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

GO

-- ERP files

CREATE TABLE bronze.erp_loc_a101 (
	cid NVARCHAR (50),
	cntry NVARCHAR (50)
);

GO

CREATE TABLE bronze.erp_cust_az12 (
	cid NVARCHAR (50),
	bdate DATE,
	gen NVARCHAR (50)
);

GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
	id NVARCHAR (50),
	cat NVARCHAR (50),
	subcat NVARCHAR (50),
	maintenance NVARCHAR (50)
);
