-- Migration V001: initial lakehouse schema
-- Run once on first deployment.

CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

-- Tables are created by DDL scripts in sql/ddl/
-- Run: spark-sql -f sql/ddl/bronze_transactions.sql
--      spark-sql -f sql/ddl/silver_customers.sql
