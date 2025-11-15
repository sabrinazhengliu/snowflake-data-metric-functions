-- manage data quality escalations on schema level to group table ownerships
CREATE OR REPLACE TABLE DATA_QUALITY_EMAIL_NOTIFICATION_CONFIG (
  DATABASE_NAME          VARCHAR
, SCHEMA_NAME            VARCHAR
, EMAIL_RECIPIENTS       VARCHAR
);

INSERT INTO DATA_QUALITY_EMAIL_NOTIFICATION_CONFIG
VALUES
  ('SLIU_DB', 'PUBLIC', '<email>');

SELECT * FROM DATA_QUALITY_EMAIL_NOTIFICATION_CONFIG;

-- create email integration
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS email_integration
  TYPE = EMAIL
  ENABLED = TRUE
;

-- add verified user
SELECT SYSTEM$START_USER_EMAIL_VERIFICATION('<user_name>');

-- create a SP to run any query and return HTML for df
CREATE OR REPLACE PROCEDURE sp_query_to_html(query_text STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.12'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run_sp'
AS
$$
from snowflake.snowpark import Session

def run_sp(session: Session, query_text: str):
    df = session.sql(query_text).to_pandas()
    html_output = df.to_html(
        index=False, 
        border=1,
        classes="table",
        justify="left"
    )
    return html_output
$$;

CALL sp_query_to_html('SELECT * FROM SLIU_DB.PUBLIC.T1');


-- create a SP to send email notification including the exceptions
CREATE OR REPLACE PROCEDURE sp_notify_exceptions(TABLE_FULL_PATH STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var EMAIL_INTEGRATION_NAME = 'email_integration';    
    var CONFIG_TABLE = 'DATA_QUALITY_EMAIL_NOTIFICATION_CONFIG';
    var EXCEPTION_TABLE_SUFFIX = '_EXCEPTION_CURRENT';
    
    var path_parts = TABLE_FULL_PATH.split('.');
    if (path_parts.length !== 3) {
        return `Error: Invalid TABLE_FULL_PATH '${TABLE_FULL_PATH}'. 
        Expected format 'DB.SCHEMA.TABLE'.`;
    }
    var db_name = path_parts[0];
    var schema_name = path_parts[1];

    var email_recipients = "";
    
    try {
        var lookup_query = `SELECT EMAIL_RECIPIENTS FROM ${CONFIG_TABLE} WHERE DATABASE_NAME = ? AND SCHEMA_NAME = ?`;
        var stmt = snowflake.createStatement({ 
            sqlText: lookup_query, 
            binds: [db_name, schema_name] 
        });
        var rs = stmt.execute();

        if (!rs.next()) {
            return `Error: No email config found in ${CONFIG_TABLE} for DB/SCHEMA: ${db_name}.${schema_name}`;
        }
    
        email_recipients = rs.getColumnValue(1);
        if (!email_recipients) {
            return `Error: Config found for ${db_name}.${schema_name}, but EMAIL_RECIPIENTS column is empty.`;
        }
    } catch (err) {
        return `Error: Failed to query config table ${CONFIG_TABLE}. Details: ${err.message}`;
    }
    var html_text = "";
    var query_text = `SELECT '${TABLE_FULL_PATH}' AS TABLE_FULL_PATH
                      , COALESCE(PARSE_JSON(ARGUMENTS)[0]:domain::STRING, 'TABLE') AS OBJECT_TYPE
                      , PARSE_JSON(ARGUMENTS)[0]:name::STRING AS COLUMN_NAME
                      , METRIC_DATABASE||'.'||METRIC_SCHEMA||'.'||METRIC_NAME AS DATA_METRIC_FUNCTION
                      , EXPECTATION_NAME, EXPECTATION_EXPRESSION, VALUE AS ACTUAL_VALUE, EXPECTATION_VIOLATED
                      FROM TABLE(SYSTEM$EVALUATE_DATA_QUALITY_EXPECTATIONS(REF_ENTITY_NAME => '${TABLE_FULL_PATH}'))
                      WHERE EXPECTATION_VIOLATED::BOOLEAN = TRUE`;
    var call_sql = "CALL sp_query_to_html(:1)";
    var stmt = snowflake.execute({
        sqlText: call_sql,
        binds: [query_text]
    });
    stmt.next(); 
    var html_output = stmt.getColumnValue(1);
    html_text += html_output;
    html_text += `<br><br><br>`;

    var exception_table_name = TABLE_FULL_PATH + EXCEPTION_TABLE_SUFFIX;
    var query_text = `SELECT * FROM ${exception_table_name}`;

    var call_sql = "CALL sp_query_to_html(:1)";
    var stmt = snowflake.execute({
        sqlText: call_sql,
        binds: [query_text]  // Bind the input variable
    });
    stmt.next(); 
    var html_output = stmt.getColumnValue(1);
    html_text += html_output;
    
    var email_subject = `Data Quality Exceptions Violated: ${TABLE_FULL_PATH} - ${new Date().toLocaleString()}`;
    
    try {
        var send_email_query = `
            CALL SYSTEM$SEND_EMAIL(
                '${EMAIL_INTEGRATION_NAME}',
                ?, 
                ?,
                ?,
                'text/html'
            )
        `;
        var email_stmt = snowflake.createStatement({
            sqlText: send_email_query,
            binds: [email_recipients, email_subject, html_text]
        });
        email_stmt.execute();
        
    } catch (err) {
        return `Error: Failed to send email via ${EMAIL_INTEGRATION_NAME}. Check integration name and permissions. Details: ${err.message}`;
    }

    return `Successfully sent exception report for ${TABLE_FULL_PATH} to ${email_recipients}.`;

} catch (err) {
    return `An unexpected error occurred: ${err.message} \nStack: ${err.stack}`;
}
$$;

CALL sp_notify_exceptions('SLIU_DB.PUBLIC.T1');

