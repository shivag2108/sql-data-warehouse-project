/*
========================================================
Create Database and Schemas
=========================================================
Script Purpose:
  This script vreates a new database names 'DataWarehouse' after checking if it already exists.
  If the database exists, it dropped and recreated. Additionally, the script sets up three schemas
  within the database: 'bronze', 'sliver', 'gold'.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS ( SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET single_user WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

--Create the 'DataWarehouse' database

CREATE DATABASE DataWarehosue;
Go

USE DataWarehouse;
GO

-- Create Schemas

CREATE SCHEMA bronze;
GO

CREATE SCHEMA sliver;
GO

CREATE SCHEMA gold;
GO


