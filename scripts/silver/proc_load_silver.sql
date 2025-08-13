/*
STORED PROCEDURE: LOAD SILVER LAYER (SOURCE -> SILVER)
SCRIPT PURPOSE:
THIS STORED PROCEDURE LOADS DATA INTO THE SILVER SCHEMA FROM EXTERNAL CSV FILES.
IT PERFORMS THE FOLLWING ACTIONS:
- TRUNCATES THE SILVER TABLES BEFORE LOADING
- USES THE BULK INSERT COMMAND TO LOAD DATA FROM CSV FILE TO SILVER TABLES

PARAMETERS:
NONE.

USAGE EXAMPLE:
  EXEC silver.load_silver;

*/



--EXEC silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '======================================================';
		PRINT ' LOADING SILVER LAYER';
		PRINT '======================================================';

		PRINT ' ------------------';
		PRINT ' LOADING CRM TABLES';
		PRINT ' ------------------';

			-- TABLE 1: silver.crm_cust_infO
			SET @start_time = GETDATE();
			PRINT' >> TRUNCATING TABLE : silver.crm_cust_info';
			TRUNCATE TABLE silver.crm_cust_info;
			PRINT' >> INSERTING DATA INTO :silver.crm_cust_info';
			INSERT INTO silver.crm_cust_info(
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
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 ELSE 'n/a'
			END cst_marital_status,
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 ELSE 'UNKNOWN'
			END cst_gndr ,
			cst_create_date
			FROM(
			SELECT
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			 FROM bronze.crm_cust_info
			)t WHERE flag_last= 1 AND  cst_id is not null;
			SET @end_time = GETDATE();
			PRINT ' >> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @start_time , @end_time) AS NVARCHAR) + 'SECONDS';
			PRINT' --------------------------------'

	-- TABLE 2: silver.crm_prd_info
	SET @start_time = GETDATE();
	PRINT' >> TRUNCATING TABLE : silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT' >> INSERTING DATA INTO :silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info(
		prd_id,
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
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,  -- extract category id
		SUBSTRING(prd_key, 7, len(prd_key)) AS prd_key, -- extract product key
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost, -- remove nulls
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'N/A'
		END AS prd_line, -- map product line codes to descriptive values
		CAST(prd_start_dt AS DATE)AS prd_start_dt,
		CAST(LEAD(prd_start_dt) over (partition by  prd_key order by prd_start_dt)-1 AS DATE 
		) as prd_end_dt -- calculate end date as one day before the next start date
	from bronze.crm_prd_info;
	SET @end_time = GETDATE();
			PRINT ' >> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @start_time , @end_time) AS NVARCHAR) + 'SECONDS';
			PRINT' --------------------------------'

	-- TABLE 3: silver.crm_sales_details
	SET @start_time = GETDATE();
	PRINT' >> TRUNCATING TABLE : silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT' >> INSERTING DATA INTO :silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE  -- handling invalid data and typecasting
			WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST (sls_order_dt AS varchar) AS DATE)
		END AS sls_order_dt,
		CASE 
			WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST (sls_ship_dt AS varchar) AS DATE)
		END AS sls_ship_dt,
		CASE 
			WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST (sls_due_dt AS varchar) AS DATE)
		END AS sls_due_dt,
		CASE 
			WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
			then sls_quantity * ABS(sls_price)
			else sls_sales -- recalculate sales if original value is missing or incorrect
		end as sls_sales,
	sls_quantity,
		CASE 
			WHEN sls_price is null or sls_price <=0 
			then sls_sales / nullif(sls_quantity,0)
			else sls_price -- derive price if original is invalid
		end as sls_price 
	from bronze.crm_sales_details;
	SET @end_time = GETDATE();
			PRINT ' >> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @start_time , @end_time) AS NVARCHAR) + 'SECONDS';
			PRINT' --------------------------------'


			SET @end_time = GETDATE();
		PRINT ' >> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @start_time , @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT' --------------------------------'

		PRINT ' ------------------';
		PRINT ' LOADING ERP TABLES';
		PRINT ' ------------------';
	-- TABLE 4: silver.erp_cust_az12
	SET @start_time = GETDATE();
	PRINT' >> TRUNCATING TABLE : silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT' >> INSERTING DATA INTO : silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
	SELECT
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid)) -- REMOVE 'NAS' PREFIX IF PRESENT
		ELSE CID
	END CID,
	CASE 
		WHEN bdate > GETDATE() THEN NULL -- SET FUTURE BIRTHDATES TO NULL
		ELSE bdate
	END AS	bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE' ) THEN 'Male'
		ELSE 'n/a' -- NORMALIZE GENDER VALUES AND HANDLE UNKNOWN CASES
	END AS gen
	FROM bronze.erp_cust_az12;
	SET @end_time = GETDATE();
		PRINT ' >> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @start_time , @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT' --------------------------------'

	-- TABLE 5: silver.erp_loc_a101
	SET @start_time = GETDATE();
	PRINT' >> TRUNCATING TABLE : silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT' >> INSERTING DATA INTO : silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101(cid,cntry)
	SELECT 
	REPLACE(cid, '-', '')cid,  -- REPLACED '-' WITH EMPTY STRING
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) in ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' or cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry   --  NORMALIZE AND HANDLE MISSING OR BLANK COUNTRY CODES.
	FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT ' >> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @start_time , @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT' --------------------------------'

	-- TABLE 6: silver.erp_px_cat_g1v2
	SET @start_time = GETDATE();
	PRINT' >> TRUNCATING TABLE : silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT' >> INSERTING DATA INTO : silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2
	(id,cat,subcat,maintenance)
	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;
	SET @end_time = GETDATE();
		PRINT ' >> LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @start_time , @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT' --------------------------------'

		SET @batch_end_time = GETDATE();
		PRINT' ================================='
		PRINT ' LOADING SILVER LAYER IS COMPLETED'
		PRINT ' TOTAL LOAD DURATION : ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'SECONDS';
		PRINT'===================================='

	END TRY 
	BEGIN CATCH
		PRINT '=============================='
		PRINT ' ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT ' ERROR MESSAGE ' + ERROR_MESSAGE();
		PRINT ' ERROR MESSAGE ; + CAST (ERROR_NUMBER() AS VARCHAR(50)'
		PRINT '=============================='
	END CATCH 
	
END 
