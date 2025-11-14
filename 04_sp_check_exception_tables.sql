CREATE OR REPLACE PROCEDURE SP_CHECK_EXCEPTION_TABLES(TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
try {
  var current_table_name = TABLE_NAME + '_EXCEPTION_CURRENT';
  var history_table_name = TABLE_NAME + '_EXCEPTION_HISTORY';
  var log_column_name = 'LOG_TIMESTAMP';

  // --- Step 1: Check/Create CURRENT table ---
  var create_current_sql = 
    `CREATE TRANSIENT TABLE IF NOT EXISTS IDENTIFIER(?) 
     LIKE IDENTIFIER(?)`;
  snowflake.createStatement({ 
    sqlText: create_current_sql, 
    binds: [current_table_name, TABLE_NAME] 
  }).execute();

  var alter_current_sql = 
    `ALTER TABLE IF EXISTS IDENTIFIER(?) 
     ADD COLUMN IF NOT EXISTS ${log_column_name} TIMESTAMP_LTZ`;
  snowflake.createStatement({ 
    sqlText: alter_current_sql, 
    binds: [current_table_name] 
  }).execute();

  // --- Step 2: Truncate CURRENT table ---
  var truncate_current_sql = `TRUNCATE TABLE IF EXISTS IDENTIFIER(?)`;
  snowflake.createStatement({ 
    sqlText: truncate_current_sql, 
    binds: [current_table_name] 
  }).execute();

  // --- Step 3: Check/Create HISTORY table ---  
  var create_history_sql = 
    `CREATE TABLE IF NOT EXISTS IDENTIFIER(?) LIKE IDENTIFIER(?)`;
  snowflake.createStatement({ 
    sqlText: create_history_sql, 
    binds: [history_table_name, current_table_name] 
  }).execute();

  // --- Step 4: Return success message ---
  return 'Exception tables ready for ' + TABLE_NAME;

} catch (err) {
  throw new Error(
    "Failed to prepare exception tables for " + TABLE_NAME + 
    ". Error: " + err.message
  );
}
$$;
