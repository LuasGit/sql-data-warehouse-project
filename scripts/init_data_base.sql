/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This scripts creates a new database named 'DataWarehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated. Additionally, the scripts sets up three schemas
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this scripts will drop the entire 'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this scripts.
*/

-- Drop and recreate the 'DataWarehouse' database
-- (Logic adapted for PostgreSQL: Terminate connections instead of SINGLE_USER)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_database WHERE datname = 'DataWarehouse') THEN
        -- Terminate all connections to the database to allow dropping it
        PERFORM pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = 'DataWarehouse' AND pid <> pg_backend_pid();

        -- Drop the database
        EXECUTE 'DROP DATABASE "DataWarehouse"';
    END IF;
END
$$;

-- Create the 'DataWarehouse' database
CREATE DATABASE "DataWarehouse";

/*
=============================================================
Create Schemas
=============================================================
*/

-- Create Schemas
CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;