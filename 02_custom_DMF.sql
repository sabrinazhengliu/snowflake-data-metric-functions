-- https://docs.snowflake.com/en/user-guide/data-quality-custom-dmfs#create-a-custom-dmf

-- construct the reference table
CREATE OR REPLACE TRANSIENT TABLE T2 (C1 INT);
INSERT INTO T2 VALUES (1), (2);
SELECT * FROM T2;

-- observe the bad records
SELECT * FROM T1;

-- this DMF uses the second table to enforce the entries in the first table
CREATE OR REPLACE DATA METRIC FUNCTION dmf_referential_check(
  arg_t1 TABLE (arg_c1 INT), arg_t2 TABLE (arg_c2 INT))
RETURNS NUMBER AS
 'SELECT COUNT(*) FROM arg_t1
  WHERE arg_c1 NOT IN (SELECT arg_c2 FROM arg_t2)';

-- this will show only custom DMFs in current schema
SHOW DATA METRIC FUNCTIONS;

-- set column match expectation
ALTER TABLE T1
  ADD DATA METRIC FUNCTION SLIU_DB.PUBLIC.dmf_referential_check
    ON (C1, TABLE (SLIU_DB.PUBLIC.T2(C1)))
    EXPECTATION exp_column_match (VALUE = 0);

-- check DMF association
SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
        REF_ENTITY_NAME => 'SLIU_DB.PUBLIC.T1',
        REF_ENTITY_DOMAIN => 'table'
    )
);

-- check data quality violation
SELECT * EXCLUDE (EXPECTATION_ID, ARGUMENTS)
, PARSE_JSON(ARGUMENTS)[0]:domain::STRING AS OBJECT_TYPE
, PARSE_JSON(ARGUMENTS)[0]:name::STRING AS OBJECT_NAME
  FROM TABLE(
    SYSTEM$EVALUATE_DATA_QUALITY_EXPECTATIONS(
      REF_ENTITY_NAME => 'SLIU_DB.PUBLIC.T1'
  ))
  WHERE EXPECTATION_VIOLATED::BOOLEAN = TRUE
;


