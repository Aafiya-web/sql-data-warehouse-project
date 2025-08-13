-- this script has all the changes and data transformations done in the bronze layer.




-- cleaning bronze.crm_cust_info
-- check for nulls or duplicates in primary key
-- expectation: no result

SELECT * 
  FROM [DataWarehouse].[bronze].[crm_cust_info]

  SELECT 
  cst_id,
  COUNT(*)
  from bronze.crm_cust_info
  group by cst_id
  having COUNT(*) > 1 or cst_id is NULL;
 -- check for unwanted spaces
-- EXPECTATION: NO RESULTS
use DataWarehouse
SELECT cst_firstname 
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname 
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key);
-- DATA STANDARDIZATION & CONSISTENCY
SELECT *
FROM bronze.crm_cust_info;



-- cleaning bronze.crm_sales_details table


select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details;
-- where sls_ord_num != TRIM(sls_ord_num);
-- where sls_prd_key NOT IN (select prd_key from silver.crm_prd_info);
-- where sls_cust_id NOT IN (select cst_id from silver.crm_cust_info);
-- check for invalid dates

-- FOR ORDER DATE
select 
NULLIF(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 
	OR LEN(sls_order_dt) != 8 
	OR sls_order_dt > 20500101 
	OR sls_order_dt < 19000101;

-- FOR SHIPPING DATE
select 
NULLIF(sls_ship_dt,0) sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0 
	OR LEN(sls_ship_dt) != 8 
	OR sls_ship_dt > 20500101 
	OR sls_ship_dt < 19000101;

-- for due date
select 
NULLIF(sls_due_dt,0) sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 
	OR LEN(sls_due_dt) != 8 
	OR sls_due_dt > 20500101 
	OR sls_due_dt < 19000101;

-- check for invalid date orders

select *
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;


-- check data consistency: between sales, qunatity and price
-- >> sales = quantity * price
-->> values must not be null ,zero or negative

select 
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;

--rules for calculations:
-- if sales is negative, zero or null , derive it using qunatity and price
-- if price is zero or null , calculate it using sales and quantity
-- if price is negative , convert it to a positive value.


select 
sls_sales AS old_sls_sales,
sls_quantity,
sls_price as old_sls_price,

CASE WHEN sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * ABS(sls_price)
	then sls_quantity * ABS(sls_price)
	else sls_sales
end as sls_sales,
CASE WHEN sls_price is null or sls_price <=0 
	then sls_sales / nullif(sls_quantity,0)
	else sls_price 
end as sls_price 

from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity<= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;



-- CLEANING BRONZE.ERP_LOC_A101
SELECT
cid,
cntry
from bronze.erp_loc_a101;

select cst_key
from silver.crm_cust_info;

-- fixing the cid
select 
REPLACE(cid, '-', '' )cid
from bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '' ) NOT IN
(select cst_key
from silver.crm_cust_info);

-- DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT cntry
from bronze.erp_loc_a101
order by cntry;

SELECT 
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) in ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' or cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END AS cntry
from bronze.erp_loc_a101;


--checking bronze.crm_prd_info
-- check for nulls or duplicates in primary key
-- expectation : no result
SELECT 
prd_id,
count(*)
FROM bronze.crm_prd_info
group by prd_id
having count(*) >1 or prd_id is null;

-- CHECK FOR UNWANTED SPACES
SELECT prd_nm
FROM bronze.crm_prd_info
where prd_nm != TRIM(prd_nm)

-- check for nulls or negative numbers
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 or  prd_cost is NULL;


--data standardization and consistency
SELECT DISTINCT prd_line
from  bronze.crm_prd_info;

-- check for invalid date orders
SELECT *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt;

select 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) over (partition by  prd_key order by prd_start_dt)-1 as prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R' , 'AC-HE-HL-U509');


-- CLEANING bronze.erp_cust_az12
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid))
	ELSE CID
	END CID,
bdate,
gen
FROM bronze.erp_cust_az12
--WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid)) -- CHECKING IF VALUES MATCH OR NOT
--ELSE CID
--END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- IDENTIFY OUT OF RANGE DATES
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < ' 1952-12-08' OR bdate > GETDATE();

-- data standardization & consistency
SELECT DISTINCT
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'MALE' ) THEN 'Male'
	ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;



-- cleaning bronze.erp_px_cat_g1v2
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;

-- check for unwanted spaces
select * from bronze.erp_px_cat_g1v2
where cat != TRIM(cat) or subcat != TRIM(subcat) or maintenance != TRIM(maintenance)

-- data standardization & consistency
select distinct
cat
from bronze.erp_px_cat_g1v2;

select distinct
subcat
from bronze.erp_px_cat_g1v2;

select distinct
maintenance
from bronze.erp_px_cat_g1v2;
