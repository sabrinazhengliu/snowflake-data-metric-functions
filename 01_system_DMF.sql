CREATE OR REPLACE TRANSIENT TABLE T1 (C1 INT, C2 VARCHAR);
SELECT * FROM T1;

ALTER TABLE T1 
  SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';  

-- this trigger only works on table
-- can apply continuous DMF monitoring on views, but with latency

ALTER TABLE T1
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ()
  EXPECTATION exp_not_empty (VALUE > 0)
  EXECUTE AS ROLE SUPPORT_ROLE;  -- optional: if DMF to be operated by a role with SELECT privilege but no ownership
;

ALTER TABLE T1
ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (C2)
  EXPECTATION exp_zero_null (VALUE = 0)
  EXECUTE AS ROLE SUPPORT_ROLE;  -- optional: if DMF to be operated by a role with SELECT privilege but no ownership
;

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

-- insert some good data and run above again
INSERT INTO T1
VALUES 
  (1, 'A')
, (2, 'B')
;

-- insert some bad data and run above again
INSERT INTO T1
VALUES
  (3, NULL)
;
