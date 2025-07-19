select top (1000) 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    coalesce(prd_cost, 0) as prd_cost,
    case 
		when upper(trim(prd_line)) = 'M' then 'Mountain'
		when upper(trim(prd_line)) = 'R' then 'Road'
		when upper(trim(prd_line)) = 'S' then 'Other Sales'
		when upper(trim(prd_line)) = 'T' then 'Touring'
		else 'n/a'
	end as prd_line,
    cast(prd_start_dt as date) as prd_start_dt,
    cast(case
    	when prd_start_dt > prd_end_dt then lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1
    	else prd_end_dt
    end as date) as prd_end_dt
from bronze.crm_prd_info;



with new_cst_key as (
	select
		cst_id,
    	cst_key ,
    	COALESCE(TRIM(cst_firstname), 'n/a') as cst_firstname,
    	COALESCE(TRIM(cst_lastname), 'n/a') as cst_lastname,
    	CASE 
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_marital_status,
		CASE 
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END AS cst_gndr,
    	cst_create_date,
	    row_number() over(partition by cst_key order by cst_create_date desc) as cst_num
	from bronze.crm_cust_info
	where cst_id is not null
)
SELECT 
	cst_id,
    cst_key ,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
FROM new_cst_key
where cst_num = 1;



select
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case 
    	when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
    	else cast(cast (sls_order_dt as varchar) as date)
    end as sls_order_dt,
    case 
    	when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
    	else cast(cast (sls_ship_dt as varchar) as date)
    end as sls_ship_dt,
    case 
    	when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
    	else cast(cast (sls_due_dt as varchar) as date)
    end as sls_due_dt,
    CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales / NULLIF(sls_quantity, 0) --agar tidak error
		ELSE sls_price 
	END AS sls_price
from bronze.crm_sales_details;



SELECT
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid, 
	CASE
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12;



SELECT
	REPLACE(cid, '-', '') AS cid, 
    CASE 
        WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')))) IN ('US', 'USA') 
            THEN 'UNITED STATES'
        WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')))) = 'DE'
            THEN 'GERMANY'
        WHEN LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), ''))) = '' OR
        LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), ''))) IS NULL THEN 'n/a'
        ELSE UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), ''))))
    END AS cntry
FROM bronze.erp_loc_a101;



SELECT
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2;

