CREATE OR REPLACE TABLE DATA_VALIDATION_RESULTS (
  LOG_TIMESTAMP          TIMESTAMP_LTZ
, DATABASE_NAME          VARCHAR
, SCHEMA_NAME            VARCHAR
, TABLE_NAME             VARCHAR
, METRIC_DATABASE        VARCHAR
, METRIC_SCHEMA          VARCHAR
, METRIC_NAME            VARCHAR
, EXPECTATION_NAME       VARCHAR
, EXPECTATION_EXPRESSION VARCHAR
, METRIC_VALUE           INTEGER
, EXPECTATION_VIOLATED   BOOLEAN
, OBJECT_TYPE            VARCHAR
, OBJECT_NAME            VARCHAR
)
;
SELECT * FROM DATA_VALIDATION_RESULTS;

CREATE OR REPLACE PROCEDURE sp_log_data_validation_results(
  LOG_TIMESTAMP TIMESTAMP_LTZ
, TABLE_FULL_PATH VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
try {

  // analyze table path into parts
  var parts = TABLE_FULL_PATH.split('.');
  if (parts.length !== 3) {
    var name_err = 
      "Invalid table full path. " + 
      "Expected format DB.SCHEMA.TABLE, but got: " + TABLE_FULL_PATH;
    throw new Error(name_err);
  }
  var db_name = parts[0];
  var schema_name = parts[1];
  var table_name = parts[2];

  // get data quality check results and insert to log table
  // include all results - TRUE and FALSE
  var insert_select_sql = 
    "INSERT INTO DATA_VALIDATION_RESULTS " +
    "SELECT " +
    "  ? " +                      // 1. log_timestamp (Bind)
    ", ? " +                      // 2. db_name (Bind)
    ", ? " +                      // 3. schema_name (Bind)
    ", ? " +                      // 4. table_name (Bind)
    ", METRIC_DATABASE, METRIC_SCHEMA, METRIC_NAME, EXPECTATION_NAME" +
    ", EXPECTATION_EXPRESSION, VALUE, EXPECTATION_VIOLATED::BOOLEAN" +   
    ", PARSE_JSON(ARGUMENTS)[0]:domain::VARCHAR AS OBJECT_TYPE" +
    ", PARSE_JSON(ARGUMENTS)[0]:name::VARCHAR AS OBJECT_NAME" +
    "  FROM TABLE(SYSTEM$EVALUATE_DATA_QUALITY_EXPECTATIONS(" +
    "  REF_ENTITY_NAME => ? ));"

  var insert_stmt = snowflake.createStatement({
    sqlText: insert_select_sql, 
      binds: [LOG_TIMESTAMP.toISOString(), db_name, schema_name, table_name
             , TABLE_FULL_PATH]
  });
  var rs = insert_stmt.execute();
  rs.next();

  var rows_inserted = rs.getColumnValue(1); 
  if (rows_inserted == 0) {
    throw new Error("No violation found in for: " + TABLE_FULL_PATH);
  }

  return "Success: " + rows_inserted + " records logged. ";

} catch (err) {
  // Re-throw any errors
  throw err;
}
$$;

CALL sp_log_data_validation_results(CURRENT_TIMESTAMP, 'SLIU_DB.PUBLIC.T1');

SELECT * FROM DATA_VALIDATION_RESULTS;
