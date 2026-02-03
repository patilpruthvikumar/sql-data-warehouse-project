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
    EXEC Silver.load_silver;
===============================================================================
*/


-- Create Procedure For Loading The Cleaned Data In The Tables : 

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	BEGIN TRY

		SET @batch_start_time = GETDATE();

		PRINT '================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================';

        -- Loading CRM Tables :

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();

										-- >>> TABLE 1 <<< 

	SET @start_time = GETDATE();

	PRINT '>> TRUNCATING TABLE : silver.crm_cust_info.....'
	TRUNCATE TABLE silver.crm_cust_info
	PRINT '>> INSERTING DATA INTO TABLE : silver.crm_cust_info....'

	-- Cleaning & Loading Data From Bronze.crm_cust_info Table To silver.crm_cust_info : 

	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

	SELECT

		cst_id,
		cst_key,

		-- Remove The Unwanted Spaces In cst_firstname , cst_lastname Columns :

		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,

		-- Normalize The Marital Status Values From Coded To Meaningful Readable Format :
	
		CASE
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'N/A'
		END AS cst_marital_satatus,

	  -- Normalize The Gender Values From Coded To Meaningful Readable Format :
	
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'N/A'
		END cst_gndr,

	  cst_create_date

	FROM (

		-- Remove Duplicates If Any :
	
		SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL) t

	-- Select The Most Recent Record Per Customer :

	WHERE flag_last = 1

	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

										-- >>> TABLE 2 <<<

	PRINT '>> TRUNCATING TABLE : silver.crm_prd_info.....'
	TRUNCATE TABLE silver.crm_prd_info
	PRINT '>> INSERTING DATA INTO TABLE : silver.crm_prd_info....'

	-- Cleaning & Loading Data From Bronze.crm_prd_info Table To silver.crm_prd_info : 

	SET @start_time = GETDATE();

	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,         
		prd_key,         
		prd_nm,        
		prd_cost,       
		prd_line,       
		prd_start_dt,    
		prd_end_dt)

	SELECT

		prd_id,

		-- Extract Category_id From The Product_key :
	
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
	
		-- Extract Product_Key From The Product_key :
	
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
	
		prd_nm,
	
		-- Check For Null Values Or Negetive Values In The Product_cost :
	
		ISNULL(prd_cost,0) AS prd_cost,
	
		-- Map The Product_line Codes To The Descriptive Values :
	
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountains'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'N/A'
		END prd_line,

		-- Change Start_date Datatype To Date From DATETIME :
	
		CAST(prd_start_dt AS DATE) AS prd_start_dt,

		-- Calculate End Date As One Day Before The Next Start Date :

		CAST(
				LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1
				AS DATE )
			AS prd_end_dt

	FROM bronze.crm_prd_info

	SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------';

										-- >>> TABLE 3 <<<

	SET @start_time = GETDATE();

	PRINT '>> TRUNCATING TABLE : silver.crm_sales_details.....'
	TRUNCATE TABLE silver.crm_sales_details
	PRINT '>> INSERTING DATA INTO TABLE : silver.crm_sales_details....'

	-- Cleaning & Loading Data From Bronze.crm_sales_details Table To silver.crm_sales_details : 

	INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price )

	SELECT
	
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,

		-- Transformation And Data Type Casting Of Order Date :
	
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) ! = 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
	
		-- Transformation And Data Type Casting Of Shipping Date :
	
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) ! = 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
	
		-- Transformation And Data Type Casting Of Due Date :
	
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) ! = 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
	
		-- Recalculated Sales If The Orginal Value Is Incorrect Or Missing :
	
		CASE WHEN sls_sales IS NULL OR sls_sales < = 0 OR sls_sales ! = sls_quantity * ABS(sls_price)
			 THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
		END sls_sales,
	
		sls_quantity,
	
		-- Derived Price If Orginal Value Is Invalid : 
	
		CASE WHEN sls_price IS NULL OR sls_price < = 0
			 THEN sls_sales / NULLIF(sls_quantity,0)
			 ELSE sls_price
		END sls_price

	FROM bronze.crm_sales_details

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';

									/* =============== END OF CRM TABLES =============== */

	-- Loading ERP Tables :

	PRINT '------------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '------------------------------------------------';

										-- >>> TABLE 4 <<<
	
	SET @start_time = GETDATE();

	PRINT '>> TRUNCATING TABLE : silver.erp_cust_az12.....'
	TRUNCATE TABLE silver.erp_cust_az12
	PRINT '>> INSERTING DATA INTO TABLE : silver.erp_cust_az12....'

	-- Cleaning & Loading Data From Bronze.erp_cust_az12 Table To silver.erp_cust_az12 : 

	INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen)

	SELECT
		-- Remove NAS Prefix If Present :

		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			 ELSE cid
		END cid,

		-- Set Future Birthdate's To NULL :

		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END bdate,

		-- Normalise Gender Values And Handle Unknown Cases :

		CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M','MALE')   THEN 'Male'
			 ELSE 'N/A'
		END gen

	FROM bronze.erp_cust_az12

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';

										-- >>> TABLE 5 <<<

	PRINT '>> TRUNCATING TABLE : silver.erp_loc_a101.....'
	TRUNCATE TABLE silver.erp_loc_a101
	PRINT '>> INSERTING DATA INTO TABLE : silver.erp_loc_a101....'

	-- Cleaning & Loading Data From Bronze.erp_loc_a101 Table To silver.erp_loc_a101 :
	
	SET @start_time = GETDATE();
	
	INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry)

	SELECT
		-- Formated Customer_id To Match With Other Tables:

		REPLACE(cid,'-',''),

		-- Normalized And Handled Missing Or Blank Country Codes :
	
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
			 ELSE TRIM(cntry)
		END cntry

	FROM bronze.erp_loc_a101

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';

										-- >>> TABLE 6 <<< 

	PRINT '>> TRUNCATING TABLE : silver.erp_px_cat_g1v2.....'
	TRUNCATE TABLE silver.erp_px_cat_g1v2
	PRINT '>> INSERTING DATA INTO TABLE : silver.erp_px_cat_g1v2....'

	 -- Cleaning & Loading Data From Bronze.erp_px_cat_g1v2 Table To silver.erp_px_cat_g1v2 :
	
	SET @start_time = GETDATE();
	
	INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance)

	SELECT
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	PRINT '>> -------------';

									/* =============== END OF ERP TABLES =============== */

	PRINT '>>> INSERTION COMPLETED <<< '

-- Time Taken TO Load The Bronze Layer :

	SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY

-- Error Message(any-if) :

	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH

END
