-- This script runs automatically when the Postgres container first starts.
-- It creates a dedicated database and user for DataHub GMS.
CREATE DATABASE datahub;
CREATE USER datahub WITH PASSWORD 'datahub_password';
GRANT ALL PRIVILEGES ON DATABASE datahub TO datahub;
