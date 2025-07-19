--- Create Database 'Datawarehouse'

USE master;

DROP DATABASE Datawarehouse;

CREATE DATABASE DataWarehouse;

DROP SCHEMA bronze;

CREATE SCHEMA bronze; 

USE Datawarehouse;

CREATE SCHEMA bronze; 


-- Cek manual schema, tidak ada tampilan di secuirty
SELECT name 
FROM sys.schemas 
WHERE name = 'bronze';
--ada

-- Cek manual schema
SELECT * FROM sys.schemas;
SELECT * FROM sys.database_principals;

--ini gagal, hanya bisa di SSMS
CREATE SCHEMA silver; 
GO
CREATE SCHEMA gold; 
GO

--ini gagal
CREATE SCHEMA silver; 
CREATE SCHEMA gold; 


--ini berhasil
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver');

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');

