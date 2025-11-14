USE DATABASE SLIU_DB;
USE SCHEMA PUBLIC;

SET my_role = 'SLIU_ROLE';
SET my_schema = 'SLIU_DB.PUBLIC';

-- get access to activate data metric functions
-- this access is required to observe data quality expectations
GRANT EXECUTE DATA METRIC FUNCTION ON ACCOUNT 
TO ROLE identifier($my_role);

-- get access to use system data metric functions
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER 
TO ROLE identifier($my_role);

-- get access to create custom data metric functions
GRANT CREATE DATA METRIC FUNCTION ON SCHEMA identifier($my_schema)
TO ROLE identifier($my_role);

-- other components
GRANT
  CREATE TABLE
, CREATE VIEW
, CREATE STORED PROCEDURE
, CREATE TASK
ON SCHEMA identifier($my_schema)
TO ROLE identifier($my_role);
