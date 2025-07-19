BULK INSERT bronze.crm_cust_info
from '/media/hadi/DATA/after_switch_de_03032025/sql_data_warehouse_project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
--skip 1 header
WITH (
	firstrow = 2,
	fieldterminator = ',',
	tablock --table-level locking hint 
)

--failed
--SQL Error [4860] [S0001]: Cannot bulk load. The file "/media/hadi/DATA..source_crm/cust_info.csv" does not exist or you don't have file access rights.
--cek bash
--ls -l '/media/hadi/DATA/after_switch_de_03032025/sql_data_warehouse_project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
--Permission: rwxr-xr-x → user bisa baca/tulis/eksekusi, group dan others bisa baca dan eksekusi
--solusinya buat folder mount


truncate bronze.crm_cust_info restart identity


BULK INSERT bronze.crm_cust_info
FROM '/var/opt/datasets/source_crm/cust_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

select * from bronze.crm_cust_info
--berhasil

BULK INSERT bronze.crm_prd_info
		FROM '/var/opt/datasets/source_crm/prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

select * from bronze.crm_prd_info 
--oke sekarang ada jika sudah dihapus pakai docker compose, test backup and restore

BULK INSERT bronze.crm_sales_details
		FROM '/var/opt/datasets/source_crm/sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

select * from bronze.crm_sales_details
--test back up with init

