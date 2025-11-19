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


-- another example: using a category mapping table, check if data is complete for category
-- create mapping table to check completeness by category
CREATE OR REPLACE TRANSIENT TABLE CATEGORY_MAPPING_TABLE (
  CATEGORY VARCHAR
, PRODUCT_ID  INTEGER
);

INSERT INTO CATEGORY_MAPPING_TABLE
VALUES
  ('Category A', '1')
, ('Category A', '2')
, ('Category A', '3')
, ('Category A', '4')
, ('Category B', '5')
, ('Category B', '6')
, ('Category B', '7')
;

SELECT * FROM CATEGORY_MAPPING_TABLE;

CREATE OR REPLACE DATA METRIC FUNCTION dmf_category_completeness_check(
  arg_t1 TABLE (arg_product INTEGER),
  arg_t2 TABLE (conf_product INTEGER, conf_category VARCHAR)
)
RETURNS NUMBER AS
'SELECT COUNT(1)
 FROM arg_t2 AS Expected
 WHERE Expected.conf_category IN (
     SELECT DISTINCT Config.conf_category
     FROM arg_t2 AS Config
     INNER JOIN arg_t1 AS Actual
       ON Config.conf_product = Actual.arg_product
 )
 AND Expected.conf_product NOT IN (
     SELECT arg_product FROM arg_t1
 )';

SHOW DATA METRIC FUNCTIONS;

ALTER TABLE T1
  ADD DATA METRIC FUNCTION SLIU_DB.PUBLIC.dmf_category_completeness_check
    ON (C1, TABLE(SLIU_DB.PUBLIC.CATEGORY_MAPPING_TABLE(PRODUCT_ID, CATEGORY)))
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
SELECT METRIC_DATABASE||'.'||METRIC_SCHEMA||'.'||METRIC_NAME AS METRIC_NAME
, EXPECTATION_NAME, EXPECTATION_EXPRESSION, VALUE, EXPECTATION_VIOLATED
, PARSE_JSON(ARGUMENTS)[0]:domain::STRING AS OBJECT_TYPE
, PARSE_JSON(ARGUMENTS)[0]:name::STRING AS OBJECT_NAME
  FROM TABLE(
    SYSTEM$EVALUATE_DATA_QUALITY_EXPECTATIONS(
      REF_ENTITY_NAME => 'SLIU_DB.PUBLIC.T1'
  ))
  WHERE EXPECTATION_VIOLATED::BOOLEAN = TRUE
;


