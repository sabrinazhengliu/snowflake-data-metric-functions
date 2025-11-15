-- final procedure: create a breakpoint if any data issue occurs
-- log the exception records and send email
CREATE OR REPLACE PROCEDURE sp_check_exceptions_notify(
  TABLE_FULL_PATH VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
// Define a custom exception for data quality violation
function DataQualityViolation(message) {
  this.message = message;
  this.name = "DataQualityViolation";
  this.stack = (new Error(message)).stack;
}
DataQualityViolation.prototype = Object.create(Error.prototype);
DataQualityViolation.prototype.constructor = DataQualityViolation;

var current_timestamp = new Date().toISOString();

// log data quality expectation snapshot to results table
try {
  var call_sql = "CALL sp_log_data_validation_results(?, ?)";
  var call_stmt = snowflake.createStatement({
    sqlText: call_sql,
    binds: [current_timestamp, TABLE_FULL_PATH]
  });
  call_stmt.execute();
} catch (err) {
    throw err;
};

try {
  var sql_query = "SELECT COUNT(*) FROM TABLE(" +
    "SYSTEM$EVALUATE_DATA_QUALITY_EXPECTATIONS(REF_ENTITY_NAME => ? ))" +
    "WHERE TO_BOOLEAN(EXPECTATION_VIOLATED) = TRUE";

  var stmt = snowflake.createStatement({
    sqlText: sql_query,
    binds: [TABLE_FULL_PATH]
  });

  var rs = stmt.execute();

  rs.next(); // Move to the first row
  var count = rs.getColumnValue(1); 

  // 5. Check the count...
  if (count > 0) {
    // If count > 0, first call the logging procedure, then send email
    try {
      var call_sql = "CALL sp_log_column_exceptions(?)";
      var call_stmt = snowflake.createStatement({
        sqlText: call_sql,
        binds: [TABLE_FULL_PATH]
      });
      call_stmt.execute();
      
      var call_sql = "CALL sp_notify_exceptions(?)";
      var call_stmt = snowflake.createStatement({
        sqlText: call_sql,
        binds: [TABLE_FULL_PATH]
      });
      call_stmt.execute();
    } catch (log_err) {
      // If logging fails, we should still throw the main error,
      // but append the logging error message for context.
      var error_message = "Error: At least 1 data quality violation " + 
                          "detected for table " + TABLE_FULL_PATH + 
                          ". ADDITIONALLY, failed to call procedure: " + 
                          log_err.message;
      throw new DataQualityViolation(error_message);
    }

    // Now, throw the original custom error, noting that logging was successful.
    var error_message = "Error: At least 1 data quality violations " +
                        "detected for table " + TABLE_FULL_PATH + 
                        ". Exception records logged. ";
    throw new DataQualityViolation(error_message);

  } else {
    // Return a success message if no data quality violation. 
    return "Success: All data quality expectation checks passed for  " + TABLE_FULL_PATH;
  }

} catch (err) {
  // Re-throw any errors to force the stored procedure to fail
    throw err;
}
$$;

call sp_check_exceptions_notify('SLIU_DB.PUBLIC.T1');
