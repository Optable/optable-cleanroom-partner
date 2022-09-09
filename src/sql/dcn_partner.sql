-- Create roles (Done by DCN operator, not Optable)
USE ROLE securityadmin;
CREATE OR REPLACE ROLE optable_snowflake_cleanroom;
GRANT ROLE optable_snowflake_cleanroom TO ROLE sysadmin;

-- Grant privileges to roles
USE ROLE accountadmin;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE optable_snowflake_cleanroom WITH GRANT OPTION;
GRANT CREATE SHARE ON ACCOUNT TO ROLE optable_snowflake_cleanroom WITH GRANT OPTION;
GRANT IMPORT SHARE ON ACCOUNT TO ROLE optable_snowflake_cleanroom WITH GRANT OPTION;
GRANT OVERRIDE SHARE RESTRICTIONS ON ACCOUNT TO ROLE optable_snowflake_cleanroom WITH GRANT OPTION;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE optable_snowflake_cleanroom WITH GRANT OPTION;

-- Assume that DCN operator pass the role to Optable)

-- DCN Partner account setup #1
USE ROLE optable_snowflake_cleanroom;

-- Create database, schema and warehouse
CREATE DATABASE IF NOT EXISTS optable_partnership;
CREATE SCHEMA IF NOT EXISTS optable_partnership.public;
CREATE OR REPLACE WAREHOUSE optable_partnership_setup warehouse_size=xsmall;
CREATE TABLE IF NOT EXISTS otpable_partnership.public.profiles
(
  identifier VARCHAR NOT NULL
);

USE warehouse optable_partnership_setup;

set dcn_slug = 'bd1';
set snowflake_partner_account_locator_id = 'TF74409';
set dcn_account_locator_id = current_account();
set dcn_partner_username = current_user();

-- disconnect partner then connect
CREATE OR REPLACE PROCEDURE optable_partnership.public.disconnect_partner(current_dcn_slug VARCHAR, current_snowflake_account_locator_id VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  // Set up local variables
  var current_account_stmt = snowflake.createStatement( {sqlText: "SELECT current_account()"} );
  var current_account_result = current_account_stmt.execute();
  current_account_result.next();
  var dcn_account_locator_id = current_account_result.getColumnValue(1);

  var dcn_partner_dcr_share = "dcn_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_SNOWFLAKE_ACCOUNT_LOCATOR_ID + "_" + dcn_account_locator_id + "_dcr_share";
  var dcn_partner_role = "dcn_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_SNOWFLAKE_ACCOUNT_LOCATOR_ID + "_" + dcn_account_locator_id + "_role";
  var dcn_partner_warehouse = "dcn_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_SNOWFLAKE_ACCOUNT_LOCATOR_ID + "_" + dcn_account_locator_id + "_warehouse";
  var dcn_partner_source_db = "dcn_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_SNOWFLAKE_ACCOUNT_LOCATOR_ID + "_" + dcn_account_locator_id + "_source_db";
  var dcn_partner_dcr_db = "dcn_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_SNOWFLAKE_ACCOUNT_LOCATOR_ID + "_" + dcn_account_locator_id + "_dcr_db";
  var snowflake_partner_dcr_db = "snowflake_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_SNOWFLAKE_ACCOUNT_LOCATOR_ID + "_" + dcn_account_locator_id + "_dcr_db";
  var snowflake_partner_source_db = "snowflake_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_SNOWFLAKE_ACCOUNT_LOCATOR_ID + "_" + dcn_account_locator_id + "_source_db";
  var statements = [
    "USE ROLE accountadmin;",
    "DROP SHARE IF EXISTS " + dcn_partner_dcr_share,
    "DROP DATABASE IF EXISTS " + dcn_partner_dcr_db,
    "DROP DATABASE IF EXISTS " + dcn_partner_source_db,
    "DROP ROLE IF EXISTS " + dcn_partner_role,
    "DROP WAREHOUSE IF EXISTS " + dcn_partner_warehouse,
    "DROP DATABASE IF EXISTS " + snowflake_partner_source_db,
    "DROP DATABASE IF EXISTS " + snowflake_partner_dcr_db
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

call optable_partnership.public.disconnect_partner($dcn_slug, $snowflake_partner_account_locator_id);


set dcn_partner_role = 'dcn_partner_' || $dcn_slug || '_' || $snowflake_partner_account_locator_id || '_' || $dcn_account_locator_id || '_role';
set dcn_partner_warehouse = 'dcn_partner_' || $dcn_slug || '_' || $snowflake_partner_account_locator_id || '_' || $dcn_account_locator_id || '_warehouse';
set dcn_partner_source_db = 'dcn_partner_' || $dcn_slug || '_' || $snowflake_partner_account_locator_id || '_' || $dcn_account_locator_id || '_source_db';
set dcn_partner_dcr_db = 'dcn_partner_' || $dcn_slug || '_' || $snowflake_partner_account_locator_id || '_' || $dcn_account_locator_id || '_dcr_db';
set dcn_partner_source_schema = $dcn_partner_source_db || '.source_schema';
set dcn_partner_source_schema_profiles = $dcn_partner_source_schema || '.profiles';
set dcn_partner_dcr_shared_schema = $dcn_partner_dcr_db || '.shared_schema';
set dcn_partner_dcr_internal_schema = $dcn_partner_dcr_db || '.internal_schema';
set dcn_partner_dcr_shared_schema_match_attempts = $dcn_partner_dcr_shared_schema || '.match_attempts';
set dcn_partner_dcr_internal_schema = $dcn_partner_dcr_db || '.internal_schema';
set dcn_partner_dcr_share = 'dcn_partner_' || $dcn_slug || '_' || $snowflake_partner_account_locator_id || '_' || $dcn_account_locator_id || '_dcr_share';
set snowflake_partner_dcr_share = $snowflake_partner_account_locator_id || '.snowflake_partner_' || $dcn_slug || '_' || $snowflake_partner_account_locator_id || '_' || $dcn_account_locator_id || '_dcr_share';
set snowflake_partner_source_share = $snowflake_partner_account_locator_id || '.snowflake_partner_' || $dcn_slug || '_' || $snowflake_partner_account_locator_id || '_' || $dcn_account_locator_id || '_source_share';
set snowflake_partner_source_db = 'snowflake_partner_' || $dcn_slug || '_' || $snowflake_partner_account_locator_id || '_' || $dcn_account_locator_id || '_source_db';
set snowflake_partner_dcr_db = 'snowflake_partner_' || $dcn_slug || '_' || $snowflake_partner_account_locator_id || '_' || $dcn_account_locator_id || '_dcr_db';


-- Create roles
-- USE ROLE securityadmin;
CREATE OR REPLACE ROLE identifier($dcn_partner_role);
GRANT ROLE identifier($dcn_partner_role) TO ROLE sysadmin;
GRANT ROLE identifier($dcn_partner_role) TO ROLE optable_snowflake_cleanroom;
GRANT ROLE identifier($dcn_partner_role) TO USER identifier($dcn_partner_username);

-- Grant privileges to roles
USE ROLE optable_snowflake_cleanroom;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE identifier($dcn_partner_role);
GRANT CREATE SHARE ON ACCOUNT TO ROLE identifier($dcn_partner_role);
GRANT IMPORT SHARE ON ACCOUNT TO ROLE identifier($dcn_partner_role);
GRANT OVERRIDE SHARE RESTRICTIONS ON ACCOUNT TO ROLE identifier($dcn_partner_role);
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE identifier($dcn_partner_role);


-- Create virtual warehouse
USE ROLE identifier($dcn_partner_role);
CREATE OR REPLACE WAREHOUSE identifier($dcn_partner_warehouse) warehouse_size=xsmall;
USE WAREHOUSE identifier($dcn_partner_warehouse);


-- Create source database and schema, along with customer table populated with synthetic data
-- Note that this dataset doesn't have any demographics - hence the need for encrichment from Party1's dataset
CREATE OR REPLACE DATABASE identifier($dcn_partner_source_db);
CREATE OR REPLACE SCHEMA identifier($dcn_partner_source_schema);

CREATE VIEW IF NOT EXISTS identifier($dcn_partner_source_schema_profiles) AS
SELECT * FROM optable_partnership.public.profiles

-- Create clean room database
CREATE OR REPLACE DATABASE identifier($dcn_partner_dcr_db);

-- Create clean room shared schema and objects
CREATE OR REPLACE SCHEMA identifier($dcn_partner_dcr_shared_schema);

CREATE OR REPLACE TABLE identifier($dcn_partner_dcr_shared_schema_match_attempts)
(
  request_id VARCHAR,
  match_id VARCHAR,
  match_attempt_id VARCHAR,
  match_result VARIANT,
  attempt_ts TIMESTAMP_TZ,
  status VARCHAR
);

-- Enable change tracking on the match_attempts table
-- This will be used by the PARTY1_DCR_DB.INTERNAL_SCHEMA.PARTY2_NEW_MATCH_ATTEMPTS Table Stream
ALTER TABLE identifier($dcn_partner_dcr_shared_schema_match_attempts)
SET CHANGE_TRACKING = TRUE
    DATA_RETENTION_TIME_IN_DAYS = 14;

-- Create outbound share
CREATE OR REPLACE SHARE identifier($dcn_partner_dcr_share);

-- Grant object privileges to DCR share
GRANT USAGE ON DATABASE identifier($dcn_partner_dcr_db) TO SHARE identifier($dcn_partner_dcr_share);
GRANT USAGE ON SCHEMA identifier($dcn_partner_dcr_shared_schema) TO SHARE identifier($dcn_partner_dcr_share);
GRANT SELECT ON TABLE identifier($dcn_partner_dcr_shared_schema_match_attempts) TO SHARE identifier($dcn_partner_dcr_share);


-- Add account to share
-- Note use of SHARE_RESTRICTIONS clause to enable sharing between Business Critical and Enterprise account deployments
USE ROLE optable_snowflake_cleanroom;
ALTER SHARE identifier($dcn_partner_dcr_share) ADD ACCOUNTS = identifier($snowflake_partner_account_locator_id) SHARE_RESTRICTIONS = FALSE;

CREATE OR REPLACE SCHEMA identifier($dcn_partner_dcr_internal_schema);


GRANT USAGE ON PROCEDURE optable_partnership.public.generate_match_request(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR,VARCHAR, REAL) TO ROLE identifier($dcn_partner_role);

-- PART 2
-- Create databases from incoming Party1 shares and grant privileges
--USE ROLE accountadmin;
USE ROLE optable_snowflake_cleanroom;
CREATE OR REPLACE DATABASE identifier($snowflake_partner_dcr_db) FROM SHARE identifier($snowflake_partner_dcr_share);
GRANT IMPORTED PRIVILEGES ON DATABASE identifier($snowflake_partner_dcr_db) TO ROLE identifier($dcn_partner_role);

CREATE OR REPLACE DATABASE identifier($snowflake_partner_source_db) FROM SHARE identifier($snowflake_partner_source_share);
GRANT IMPORTED PRIVILEGES ON DATABASE identifier($snowflake_partner_source_db) TO ROLE identifier($dcn_partner_role);
