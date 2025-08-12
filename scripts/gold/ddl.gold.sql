/*
 ======================================================================================
  DDL Script: Create Gold Views
 ========================================================================================
Script Purpose:
  This script creates views for the Gold Layer in the date warehouse.
  The gold layer represets the final dimennsion and fact tables (star schema)

  Each view perfomns transformations and combines data from the sliver layer
  to produce a clean , enriched , and business -ready dataset.

  Usage:
  - These views can be queried directly for analytics and reporting.
============================================================================================
*/

  -- ***************************************************************************************
 -- Create Dimension Table: gold.dim_customers
-- ****************************************************************************************

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
ON ci.cst_key = la.cid;

SELECT * FROM gold.dim_customers;
  
-- ***********************************************************************************
 -- Create Dimension Table: gold.dim_products
-- ***********************************************************************************

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
WHERE pn.prd_end_dt IS NULL;

SELECT * FROM gold.dim_products;

-- ***********************************************************************************
 -- Create Dimension Table: gold.fact_sales
-- ***********************************************************************************

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
ON sd.sls_cust_id = cu.customer_id;

SELECT * FROM gold.fact_sales;

