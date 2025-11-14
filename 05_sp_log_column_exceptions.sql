-- capture column-level DMF violations, system DMF only
SELECT
    PARSE_JSON(REF_ARGUMENTS)[0]:name::VARCHAR AS COLUMN_NAME,
    METRIC_DATABASE_NAME||'.'||METRIC_SCHEMA_NAME||'.'||METRIC_NAME AS METRIC_NAME
    FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
      REF_ENTITY_NAME => 'SLIU_DB.PUBLIC.T1',
      REF_ENTITY_DOMAIN => 'table'
    ))
    WHERE COLUMN_NAME IS NOT NULL
    AND METRIC_DATABASE_NAME = 'SNOWFLAKE';


CREATE OR REPLACE PROCEDURE sp_log_column_exceptions(TABLE_FULL_PATH VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER  -- required for querying INFORMATION_SCHEMA
AS
$$
try {
  // --- Step 1: Call procedure to preprare exception tables ---
  snowflake.createStatement({
    sqlText: "CALL sp_check_exception_tables(?)",
    binds: [TABLE_FULL_PATH]
  }).execute();

  // --- Step 2: Get column-level metric association ---
  var config_sql = `SELECT
    PARSE_JSON(REF_ARGUMENTS)[0]:name::VARCHAR AS COLUMN_NAME,
    METRIC_DATABASE_NAME||'.'||METRIC_SCHEMA_NAME||'.'||METRIC_NAME AS METRIC_NAME
    FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
      REF_ENTITY_NAME => ?,
      REF_ENTITY_DOMAIN => 'table'
    ))
    WHERE COLUMN_NAME IS NOT NULL
    AND METRIC_DATABASE_NAME = 'SNOWFLAKE'`;

  var config_stmt = snowflake.createStatement({
    sqlText: config_sql,
    binds: [TABLE_FULL_PATH]
  });
  var config_rs = config_stmt.execute();

  var metrics_run_count = 0;

  // --- Step 3: Loop through all column-metric rows ---
  while (config_rs.next()) {
    var column_name = config_rs.getColumnValue(1);
    var metric_name = config_rs.getColumnValue(2);

    // --- Step 4: Compose the dynamic source query ---
    var source_query_sql = 
      "SELECT *, CURRENT_TIMESTAMP AS LOG_TIMESTAMP " +
      "FROM TABLE(SYSTEM$DATA_METRIC_SCAN(" +
      "  REF_ENTITY_NAME => ?" +  // Bind: TABLE_FULL_PATH
      ", METRIC_NAME => ?" +      // Bind: metric_name
      ", ARGUMENT_NAME => ?" +    // Bind: column_name
      "))";

    // --- Step 5: Insert into CURRENT table ---
    var current_table_name = TABLE_FULL_PATH + '_EXCEPTION_CURRENT';
    var insert_current_sql = "INSERT INTO IDENTIFIER(?) (" + source_query_sql + ")";

    snowflake.createStatement({
      sqlText: insert_current_sql,
      binds: [current_table_name, TABLE_FULL_PATH, metric_name, column_name]
    }).execute();

    // --- Step 6: Insert into HISTORY table ---
    var history_table_name = TABLE_FULL_PATH + '_EXCEPTION_HISTORY';
    var insert_history_sql = "INSERT INTO IDENTIFIER(?) (" + source_query_sql + ")";
    
    snowflake.createStatement({
      sqlText: insert_history_sql,
      binds: [history_table_name, TABLE_FULL_PATH, metric_name, column_name]
    }).execute();

    metrics_run_count++;
  }

  // --- Step 7: Return success message ---
  return "Exception Records captured for " + TABLE_FULL_PATH

} catch (err) {
  // Re-throw any errors
  throw err;
}
$$;

-- repeatedly call this procedure to observe current and history tables
CALL sp_log_column_exceptions('SLIU_DB.PUBLIC.T1');

SELECT * FROM SLIU_DB.PUBLIC.T1_EXCEPTION_CURRENT;
SELECT * FROM SLIU_DB.PUBLIC.T1_EXCEPTION_HISTORY;
