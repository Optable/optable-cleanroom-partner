-- DCN Partner account setup #1
USE ROLE accountadmin;
CREATE DATABASE IF NOT EXISTS optable_partnership;
CREATE SCHEMA IF NOT EXISTS optable_partnership.public;
CREATE OR REPLACE WAREHOUSE optable_partnership_setup warehouse_size=xsmall;
USE warehouse optable_partnership_setup;

set dcn_slug = 'bd1';
set snowflake_partner_account_id = 'TF74409';
set dcn_account_id = current_account();
set dcn_partner_username = current_user();

CREATE OR REPLACE PROCEDURE optable_partnership.public.disconnect_partner(current_dcn_slug VARCHAR, current_snowflake_account_id VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  var dcn_partner_dcr_share = "dcn_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_SNOWFLAKE_ACCOUNT_ID + "_dcr_share";
  var dcn_partner_role = "dcn_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_SNOWFLAKE_ACCOUNT_ID + "_role";
  var dcn_partner_warehouse = "dcn_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_SNOWFLAKE_ACCOUNT_ID + "_warehouse";
  var dcn_partner_source_db = "dcn_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_SNOWFLAKE_ACCOUNT_ID + "_source_db";
  var dcn_partner_dcr_db = "dcn_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_SNOWFLAKE_ACCOUNT_ID + "_dcr_db";
  var statements = [
    "USE ROLE accountadmin;",
    "DROP SHARE IF EXISTS " + dcn_partner_dcr_share,
    "DROP DATABASE IF EXISTS " + dcn_partner_dcr_db,
    "DROP DATABASE IF EXISTS " + dcn_partner_source_db,
    "DROP ROLE IF EXISTS " + dcn_partner_role,
    "DROP WAREHOUSE IF EXISTS " + dcn_partner_warehouse
  ];
  try {
    for (const stmt of statements) {
      var sql = snowflake.createStatement( {sqlText: stmt} );
      sql.execute();
    }
  } catch (err) {
    var result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
    result += "\n  Message: " + err.message;
    result += "\nStack Trace:\n" + err.stackTraceTxt;
    return result;
  }
  return 'Disconnected';
$$
;

call optable_partnership.public.disconnect_partner($dcn_slug, $snowflake_partner_account_id);

set dcn_partner_role = 'dcn_partner_' || $dcn_slug || '_' || $snowflake_partner_account_id || '_role';
set dcn_partner_warehouse = 'dcn_partner_' || $dcn_slug || '_' || $snowflake_partner_account_id || '_warehouse';
set dcn_partner_source_db = 'dcn_partner_' || $dcn_slug || '_' || $snowflake_partner_account_id || '_source_db';
set dcn_partner_dcr_db = 'dcn_partner_' || $dcn_slug || '_' || $snowflake_partner_account_id || '_dcr_db';
set dcn_partner_source_schema = $dcn_partner_source_db || '.source_schema';
set dcn_partner_source_schema_profiles = $dcn_partner_source_schema || '.profiles';
set dcn_partner_dcr_shared_schema = $dcn_partner_dcr_db || '.shared_schema';
set dcn_partner_dcr_internal_schema = $dcn_partner_dcr_db || '.internal_schema';
set dcn_partner_dcr_shared_schema_query_requests = $dcn_partner_dcr_shared_schema || '.query_requests';
set dcn_partner_dcr_shared_schema_match_attempts = $dcn_partner_dcr_shared_schema || '.match_attempts';
set dcn_partner_dcr_internal_schema = $dcn_partner_dcr_db || '.internal_schema';
set dcn_partner_dcr_share = 'dcn_partner_' || $dcn_slug || '_' || $snowflake_partner_account_id || '_dcr_share';
set snowflake_partner_dcr_share = $snowflake_partner_account_id || '.snowflake_partner_' || $dcn_slug || '_' || $dcn_account_id || '_dcr_share';
set snowflake_partner_source_share = $snowflake_partner_account_id || '.snowflake_partner_' || $dcn_slug || '_' || $dcn_account_id || '_source_share';
set snowflake_partner_source_db = 'snowflake_partner_' || $dcn_slug || '_' || $dcn_account_id || '_source_db';
set snowflake_partner_dcr_db = 'snowflake_partner_' || $dcn_slug || '_' || $dcn_account_id || '_dcr_db';

-- Create roles
USE ROLE securityadmin;
CREATE OR REPLACE ROLE identifier($dcn_partner_role);
GRANT ROLE identifier($dcn_partner_role) TO ROLE sysadmin;
GRANT ROLE identifier($dcn_partner_role) TO USER identifier($dcn_partner_username);

-- Grant privileges to roles
USE ROLE accountadmin;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE identifier($dcn_partner_role);
GRANT CREATE SHARE ON ACCOUNT TO ROLE identifier($dcn_partner_role);
GRANT IMPORT SHARE ON ACCOUNT TO ROLE identifier($dcn_partner_role);
GRANT EXECUTE TASK ON ACCOUNT TO ROLE identifier($dcn_partner_role);
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE identifier($dcn_partner_role);

-- Create virtual warehouse
USE ROLE identifier($dcn_partner_role);
CREATE OR REPLACE WAREHOUSE identifier($dcn_partner_warehouse) warehouse_size=xsmall;
USE WAREHOUSE identifier($dcn_partner_warehouse);

-- Create source database and schema, along with customer table populated with synthetic data
-- Note that this dataset doesn't have any demographics - hence the need for encrichment from Party1's dataset
CREATE OR REPLACE DATABASE identifier($dcn_partner_source_db);
CREATE OR REPLACE SCHEMA identifier($dcn_partner_source_schema);

CREATE OR REPLACE TABLE identifier($dcn_partner_source_schema_profiles)
(
  identifier VARCHAR NOT NULL
);

-- Create clean room database
CREATE OR REPLACE DATABASE identifier($dcn_partner_dcr_db);

-- Create clean room shared schema and objects
CREATE OR REPLACE SCHEMA identifier($dcn_partner_dcr_shared_schema);

-- Create query requests table
CREATE OR REPLACE TABLE identifier($dcn_partner_dcr_shared_schema_query_requests)
(
  request_id VARCHAR,
  target_table_name VARCHAR,
  query_template_name VARCHAR,
  match_attempt_id VARCHAR,
  at_timestamp VARCHAR,
  request_ts TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE identifier($dcn_partner_dcr_shared_schema_match_attempts)
(
  match_attempt_id VARCHAR,
  match_result VARCHAR
);

-- Enable change tracking on the query requests table
-- This will be used by the PARTY1_DCR_DB.INTERNAL_SCHEMA.PARTY2_NEW_REQUESTS Table Stream
ALTER TABLE identifier($dcn_partner_dcr_shared_schema_query_requests)
SET CHANGE_TRACKING = TRUE
    DATA_RETENTION_TIME_IN_DAYS = 14;

-- Create outbound share
CREATE OR REPLACE SHARE identifier($dcn_partner_dcr_share);

-- Grant object privileges to DCR share
GRANT USAGE ON DATABASE identifier($dcn_partner_dcr_db) TO SHARE identifier($dcn_partner_dcr_share);
GRANT USAGE ON SCHEMA identifier($dcn_partner_dcr_shared_schema) TO SHARE identifier($dcn_partner_dcr_share);
GRANT SELECT ON TABLE identifier($dcn_partner_dcr_shared_schema_query_requests) TO SHARE identifier($dcn_partner_dcr_share);
GRANT SELECT ON TABLE identifier($dcn_partner_dcr_shared_schema_match_attempts) TO SHARE identifier($dcn_partner_dcr_share);

-- Add account to share
-- Note use of SHARE_RESTRICTIONS clause to enable sharing between Business Critical and Enterprise account deployments
USE ROLE ACCOUNTADMIN;
ALTER SHARE identifier($dcn_partner_dcr_share) ADD ACCOUNTS = identifier($snowflake_partner_account_id) SHARE_RESTRICTIONS=false;

CREATE OR REPLACE SCHEMA identifier($dcn_partner_dcr_internal_schema);

-- Create query request generation stored procedure
CREATE OR REPLACE PROCEDURE optable_partnership.public.generate_match_request(current_dcn_slug VARCHAR, current_snowflake_account_id VARCHAR, query_template_name VARCHAR, match_attempt_id VARCHAR, at_timestamp VARCHAR, wait_minutes REAL)
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS
$$
// GENERATE_QUERY_REQUEST - Michael Rainey and Rachel Blum
// Adapted for Quickstart by Craig Warman
// Snowflake Computing, MAR 2022
//
// This stored procedure generates query requests and submits them to the QUERY_REQUESTS
// table in a simple two-party Snowflake Data Clean Room (DCR) deployment.   It is provided
// for illustrative purposes only as part of the "Build A Data Clean Room in Snowflake"
// Quickstart lab, and MUST NOT be used in a production environment.
//

try {
  // Set up local variables
  var current_account_stmt = snowflake.createStatement( {sqlText: "SELECT current_account()"} );
  var current_account_result = current_account_stmt.execute();
  current_account_result.next();
  var dcn_account_id = current_account_result.getColumnValue(1);

  var dcr_db_internal_schema_name = "dcn_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_SNOWFLAKE_ACCOUNT_ID + "_dcr_db.internal_schema";
  var dcr_db_shared_schema_name_in = "snowflake_partner_" + CURRENT_DCN_SLUG + "_" + dcn_account_id + "_dcr_db.shared_schema";
  var dcr_db_shared_schema_name_out = "dcn_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_SNOWFLAKE_ACCOUNT_ID + "_dcr_db.shared_schema";

  // Get parameters
  var match_attempt_id = MATCH_ATTEMPT_ID;
  var target_table_name = 'profiles';
  var query_template_name = QUERY_TEMPLATE_NAME;
  var at_timestamp = AT_TIMESTAMP;
  var wait_minutes = WAIT_MINUTES;

  var timeout = wait_minutes * 60 * 1000; // Note that this is specified in milliseconds, hence the need to multiply the WAIT_MINUTES parameter value accordingly

  // Fetch a UUID string for use as a Result ID.
  var UUID_sql = "SELECT replace(UUID_STRING(),'-','_');";
  var UUID_statement = snowflake.createStatement( {sqlText: UUID_sql} );
  var UUID_result = UUID_statement.execute();
  UUID_result.next();
  var request_id = UUID_result.getColumnValue(1);

  // Generate the request and insert into the QUERY_REQUESTS table.
  var insert_request_sql = "INSERT INTO " + dcr_db_shared_schema_name_out + ".query_requests \
							 (request_id, target_table_name, query_template_name, match_attempt_id, at_timestamp, request_ts) \
						   VALUES \
							 ( \
							   '" + request_id + "', \
							   \$\$" + target_table_name + "\$\$, \
							   \$\$" + query_template_name + "\$\$, \
							   \$\$" + match_attempt_id + "\$\$, \
							   \$\$" + at_timestamp + "\$\$, \
							   CURRENT_TIMESTAMP() \
							 );";

  var insert_request_statement = snowflake.createStatement( {sqlText: insert_request_sql} );
  var insert_request_result = insert_request_statement.execute();


  // Poll the REQUEST_STATUS table until the request is complete or the timeout period has expired.
  // Note that this is fine for an interactive demo but wouldn't be a good practice for a production deployment.
  var request_status_sql = "SELECT request_status, comments, query_text, target_table_name FROM " + dcr_db_shared_schema_name_in + ".request_status \
                            WHERE request_id = '" + request_id + "' ORDER BY request_status_ts DESC LIMIT 1;";
  var request_status_statement = snowflake.createStatement( {sqlText: request_status_sql} );

  var startTimestamp = Date.now();
  var currentTimestamp = null;
  do {
	  currentTimestamp = Date.now();
	  var request_status_result =  request_status_statement.execute();
  } while ((request_status_statement.getRowCount() < 1) && (currentTimestamp - startTimestamp < timeout));


  // Exit with message if the wait time has been exceeded.
  if ((request_status_statement.getRowCount() < 1) && (currentTimestamp - startTimestamp >= timeout)) {
	  return "Unfortunately the wait time of " + wait_minutes.toString() + " minutes expired before the other party reviewed the query request.  Please try again.";
  }

  // Examine the record fetched from the REQUEST_STATUS table.
  request_status_result.next();
  var status = request_status_result.getColumnValue(1);
  var comments = request_status_result.getColumnValue(2);
  var query_text = request_status_result.getColumnValue(3);
  var target_table_name = request_status_result.getColumnValue(4);

  if (status != "APPROVED") {
	  return "The other party DID NOT approve the query request.  Comments: " + comments;
  }

  // The query request was approved.
  // First, set context to the DCR internal schema...
  var use_schema_sql = "USE " + "SCHEMA " + dcr_db_internal_schema_name + ";";  // Have to separate "USE" and "SCHEMA" strings due to Snowsight bug.
  var use_schema_statement = snowflake.createStatement( {sqlText: use_schema_sql} );
  var use_schema_result = use_schema_statement.execute();

  // Then execute the approved query.
  var approved_query_statement = snowflake.createStatement( {sqlText: query_text} );
  var approved_query_result = approved_query_statement.execute();
  return "The other party APPROVED the query request.  Its results are now available this table: " + MATCH_ATTEMPT_ID.toUpperCase() + "_" +dcr_db_internal_schema_name.toUpperCase() + "." + target_table_name.toUpperCase();

}
catch (err) {
    var result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
    result += "\n  Message: " + err.message;
    result += "\nStack Trace:\n" + err.stackTraceTxt;
    return result;
}
$$
;

-- PART 2
-- Create databases from incoming Party1 shares and grant privileges
USE ROLE accountadmin;
CREATE OR REPLACE DATABASE identifier($snowflake_partner_dcr_db) FROM SHARE identifier($snowflake_partner_dcr_share);
GRANT IMPORTED PRIVILEGES ON DATABASE identifier($snowflake_partner_dcr_db) TO ROLE identifier($dcn_partner_role);

CREATE OR REPLACE DATABASE identifier($snowflake_partner_source_db) FROM SHARE identifier($snowflake_partner_source_share);
GRANT IMPORTED PRIVILEGES ON DATABASE identifier($snowflake_partner_source_db) TO ROLE identifier($dcn_partner_role);
