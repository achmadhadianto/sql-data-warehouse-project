=============================
--ALL TABLE
=============================

select top (1000) * from bronze.crm_cust_info;
select top (1000) * from bronze.crm_prd_info;
select top (1000) * from bronze.crm_sales_details;
select top (1000) * from bronze.erp_cust_az12;
select top (1000) * from bronze.erp_loc_a101;
select top (1000) * from bronze.erp_px_cat_g1v2;



=============================
--ALL COLUMN TABLE
=============================

select top (1000) 
	cst_id,
    cst_key ,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
from bronze.crm_cust_info;

select top (1000) 
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
from bronze.crm_prd_info;

select top (1000) 
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

select top (1000)
	cid,
    bdate,
    gen
from bronze.erp_cust_az12;

select top (1000) 
    cid,
    cntry
from bronze.erp_loc_a101;

select top (1000)
	id,
    cat,
    subcat,
    maintenance
from bronze.erp_px_cat_g1v2;


=============================
--UNIQUE CHECK
=============================

SELECT sls_prd_key, count(sls_prd_key) FROM bronze.crm_sales_details
GROUP BY sls_prd_key
HAVING COUNT(sls_prd_key) > 1
ORDER BY COUNT(sls_prd_key) DESC; --case sls_prd_key = 'WB-H098'

SELECT * FROM bronze.crm_sales_details
WHERE sls_prd_key = 'WB-H098'

SELECT sls_cust_id, count(sls_cust_id) FROM bronze.crm_sales_details
GROUP BY sls_cust_id
HAVING COUNT(sls_cust_id) > 1
ORDER BY COUNT(sls_cust_id) DESC; --case sls_cust_id = '11185'

SELECT * FROM bronze.crm_sales_details
WHERE sls_cust_id = '11185'

--test two unique
SELECT sls_prd_key, sls_cust_id, count(*) FROM bronze.crm_sales_details
GROUP BY sls_prd_key, sls_cust_id
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC; --case sls_prd_key = 'TT-T092' AND sls_cust_id = '11566'

SELECT * FROM bronze.crm_sales_details
WHERE sls_prd_key = 'TT-T092' AND sls_cust_id = '11566' --add unique sls_ord_num

SELECT sls_prd_key, sls_cust_id, sls_ord_num, count(*) FROM bronze.crm_sales_details
GROUP BY sls_prd_key, sls_cust_id, sls_ord_num 
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC; --ok


SELECT prd_key, count(prd_key) FROM bronze.crm_prd_info
GROUP BY prd_key
HAVING COUNT(prd_key) > 1
ORDER BY COUNT(prd_key) DESC; --case prd_key = 'AC-HE-HL-U509'

select * from bronze.crm_prd_info
where prd_key = 'AC-HE-HL-U509' --add unique prd_cost

SELECT prd_key, prd_cost, count(*) FROM bronze.crm_prd_info
GROUP BY prd_key, prd_cost
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC; --ok


SELECT cst_id, count(cst_id) FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id) > 1
ORDER BY COUNT(cst_id) DESC; --case cst_id = '29466'

select *, row_number() over(partition by cst_key order by cst_create_date desc) as cst_num from bronze.crm_cust_info
where cst_id = '29466'

with new_cst_key as (
	select
		*,
	    row_number() over(partition by cst_key order by cst_create_date desc) as cst_num
	from bronze.crm_cust_info
)
SELECT cst_id, count(cst_id) FROM new_cst_key
where cst_num = 1
GROUP BY cst_id
HAVING COUNT(cst_id) > 1
ORDER BY COUNT(cst_id) DESC; --ok


SELECT id, count(id) FROM bronze.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(id) > 1
ORDER BY COUNT(id) DESC;


SELECT cid, count(cid) FROM bronze.erp_loc_a101
GROUP BY cid
HAVING COUNT(cid) > 1
ORDER BY COUNT(cid) DESC;


SELECT cid, count(cid) FROM bronze.erp_cust_az12
GROUP BY cid
HAVING COUNT(cid) > 1
ORDER BY COUNT(cid) DESC;


=============================
--HANDLING NULL
=============================

SELECT sls_prd_key, sls_cust_id, sls_ord_num FROM bronze.crm_sales_details
where sls_prd_key is null or sls_cust_id is null or sls_ord_num  is null;

SELECT prd_key, prd_cost FROM bronze.crm_prd_info
where prd_key is null or prd_cost is null; --case prd_key in ('CO-RF-FR-R92B-58', 'CO-RF-FR-R92R-58') and prd_cost is null


select * from bronze.crm_prd_info cpi 
where prd_key in ('CO-RF-FR-R92B-58', 'CO-RF-FR-R92R-58') and prd_cost is null

with new_prd_cost as (
	select
	    prd_id,
	    prd_key,
	    prd_nm,
	    case
		    when prd_cost is null then 0
		    else prd_cost
		end as prd_cost,
	    prd_line,
	    prd_start_dt,
	    prd_end_dt
	from bronze.crm_prd_info
)
select * from new_prd_cost 
where prd_key is null or prd_cost is null; --ok

SELECT cst_key, cst_create_date from bronze.crm_cust_info
where cst_key is null or cst_create_date is null;

SELECT id FROM bronze.erp_px_cat_g1v2
where id is null;

SELECT cid FROM bronze.erp_loc_a101
where cid is null;

SELECT cid FROM bronze.erp_cust_az12
where cid is null;


=============================
--KEY NORMALIZATION
=============================

select top (1000) * from bronze.erp_px_cat_g1v2;

select top (1000) * from bronze.crm_prd_info; --prd_key like '%BK-R93R-62%' => BI-RB-BK-R93R-62
select top (1000) * from bronze.crm_sales_details; 

select DISTINCT
	CASE 
		WHEN replace(SUBSTRING(prd_key, 1, 5), '-', '_') = 'CO_PE' THEN 'CO_PD'
		ELSE replace(SUBSTRING(prd_key, 1, 5), '-', '_')
	END
	 as new_prd_key from bronze.crm_prd_info
where 
	CASE 
		WHEN replace(SUBSTRING(prd_key, 1, 5), '-', '_') = 'CO_PE' THEN 'CO_PD'
		ELSE replace(SUBSTRING(prd_key, 1, 5), '-', '_')
	END
not in (select distinct id from bronze.erp_px_cat_g1v2); --oke


select distinct sls_prd_key from bronze.crm_sales_details
where sls_prd_key not in (select DISTINCT --prd_key from bronze.crm_prd_info
	SUBSTRING(prd_key, 7, LEN(prd_key)) as new_prd_key from bronze.crm_prd_info);-- oke
	
select DISTINCT --prd_key from bronze.crm_prd_info
	SUBSTRING(prd_key, 7, LEN(prd_key)) as new_prd_key from bronze.crm_prd_info
where 
	SUBSTRING(prd_key, 7, LEN(prd_key))
not in (select distinct sls_prd_key from bronze.crm_sales_details); --memang tidak harus oke



select top (1000) * from bronze.crm_cust_info;
select top (1000) * from bronze.crm_sales_details;

select distinct cst_id from bronze.crm_cust_info
where cst_id not in (select distinct sls_cust_id from bronze.crm_sales_details); --oke

select distinct sls_cust_id from bronze.crm_sales_details
where sls_cust_id not in (select distinct sls_cust_id from bronze.crm_cust_info); --oke

--setiap orang terdata dan beli, tapi tidak setiap orang beli semua product



select top (1000) * from bronze.crm_cust_info;
select top (1000) * from bronze.erp_cust_az12;
select top (1000) * from bronze.erp_loc_a101;

select distinct cst_key from bronze.crm_cust_info
where cst_key not in (select distinct substring(cid, 4, len(cid)) as cid from bronze.erp_cust_az12); --oke
--banyak yang tidak ada, berarti tidak mengisi bdate dan gen

select distinct cst_key from bronze.crm_cust_info
where cst_key not in (select distinct replace(cid, '-', '') from bronze.erp_loc_a101); --oke
--hanya 3 tanpa AW yang tidak mengisi loc country


=============================
--CLEANSING OTHER COLUMN
=============================

SELECT sls_prd_key, sls_cust_id, sls_ord_num FROM bronze.crm_sales_details
where sls_prd_key is null or sls_cust_id is null or sls_ord_num  is null;

SELECT sls_order_dt, sls_ship_dt, sls_due_dt FROM bronze.crm_sales_details
where sls_order_dt is null or sls_order_dt = 0 or len(sls_order_dt) != 8
or sls_ship_dt is null or sls_ship_dt = 0 or len(sls_ship_dt) != 8
or sls_due_dt  is null or sls_due_dt = 0 or len(sls_due_dt) != 8;

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

select * from bronze.crm_sales_details
where sls_sales is null or sls_sales != abs(sls_sales);

select * from bronze.crm_sales_details
where sls_quantity is null or sls_quantity != abs(sls_quantity); --oke

select * from bronze.crm_sales_details
where sls_price is null or sls_price != abs(sls_price);





SELECT prd_key, prd_cost FROM bronze.crm_prd_info
where prd_key is null or prd_cost is null; --case prd_key in ('CO-RF-FR-R92B-58', 'CO-RF-FR-R92R-58') and prd_cost is null


select * from bronze.crm_prd_info cpi 
where prd_key in ('CO-RF-FR-R92B-58', 'CO-RF-FR-R92R-58') and prd_cost is null





select top (1000) 
    prd_id,
    prd_key,
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
from bronze.crm_prd_info
order by prd_key, prd_nm;

select distinct prd_end_dt from bronze.crm_prd_info
where prd_start_dt is null or prd_start_dt > prd_end_dt; --case prd_end_dt in ('2007-12-28 00:00:00.000', '2008-12-27 00:00:00.000') or prd_end_dt is null

select * from bronze.crm_prd_info
where prd_end_dt in ('2007-12-28 00:00:00.000', '2008-12-27 00:00:00.000') --case prd_key = 'AC-HE-HL-U509-R' and prd_nm = 'Sport-100 Helmet- Red'

select * from bronze.crm_prd_info
where prd_key = 'AC-HE-HL-U509-R' and prd_nm = 'Sport-100 Helmet- Red'

212	AC-HE-HL-U509-R	Sport-100 Helmet- Red	12	S 	2011-07-01 00:00:00.000	2007-12-28 00:00:00.000
213	AC-HE-HL-U509-R	Sport-100 Helmet- Red	14	S 	2012-07-01 00:00:00.000	2008-12-27 00:00:00.000
214	AC-HE-HL-U509-R	Sport-100 Helmet- Red	13	S 	2013-07-01 00:00:00.000	

select *, lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 as new_prd_end_dt from bronze.crm_prd_info
where prd_key = 'AC-HE-HL-U509-R' and prd_nm = 'Sport-100 Helmet- Red'

212	AC-HE-HL-U509-R	Sport-100 Helmet- Red	12	S 	2011-07-01 00:00:00.000	2007-12-28 00:00:00.000	2012-06-30 00:00:00.000
213	AC-HE-HL-U509-R	Sport-100 Helmet- Red	14	S 	2012-07-01 00:00:00.000	2008-12-27 00:00:00.000	2013-06-30 00:00:00.000
214	AC-HE-HL-U509-R	Sport-100 Helmet- Red	13	S 	2013-07-01 00:00:00.000	



SELECT cst_key, cst_create_date from bronze.crm_cust_info
where cst_key is null or cst_create_date is null;

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

SELECT cst_firstname from bronze.crm_cust_info
where cst_firstname is null ;




SELECT id FROM bronze.erp_px_cat_g1v2
where id is null;

SELECT cid FROM bronze.erp_loc_a101
where cid is null;

SELECT cid FROM bronze.erp_cust_az12
where cid is null;





select top (1000) 
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

select top (1000)
	cid,
    bdate,
    gen
from bronze.erp_cust_az12;


select
    cid,
    cntry
from bronze.erp_loc_a101;

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


    CASE 
        WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')))) IN ('US', 'USA') 
            THEN 'UNITED STATES'
        WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')))) = 'DE'
            THEN 'GERMANY'
        WHEN LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), ''))) = '' OR
        LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), ''))) IS NULL THEN 'n/a'
        ELSE UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), ''))))
    END AS cntry

select distinct TRIM(cntry) FROM bronze.erp_loc_a101;


SELECT cntry, LEN(cntry) AS length, LEN(TRIM(cntry)) AS trimmed_length
FROM bronze.erp_loc_a101
WHERE LEN(cntry) != LEN(TRIM(cntry));

SELECT 
    cntry,
    '>' + cntry + '<' AS visible_text,  -- biar kelihatan spasi
    LEN(cntry) AS length,
    LEN(LTRIM(RTRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')))) AS cleaned_length
FROM bronze.erp_loc_a101
WHERE LEN(cntry) != LEN(TRIM(cntry));

SELECT distinct cntry, 
       UNICODE(SUBSTRING(cntry, 1, 1)) AS first_char,
       UNICODE(SUBSTRING(cntry, 2, 1)) AS second_char,
       UNICODE(SUBSTRING(cntry, 3, 1)) AS third_char
FROM bronze.erp_loc_a101
WHERE LEN(cntry) > 0;


SELECT DISTINCT
    LTRIM(RTRIM(
        REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')
    )) AS cntry
FROM bronze.erp_loc_a101;


SELECT DISTINCT
    CASE 
        WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')))) IN ('US', 'USA') 
            THEN 'UNITED STATES'
        WHEN UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')))) = 'DE'
            THEN 'GERMANY'
        ELSE UPPER(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), ''))))
    END AS cntry
FROM bronze.erp_loc_a101;



select top (1000)
	id,
    cat,
    subcat,
    maintenance
from bronze.erp_px_cat_g1v2;
