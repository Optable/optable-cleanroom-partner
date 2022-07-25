-- Snowflake Partner account setup #1
USE ROLE accountadmin;
CREATE DATABASE IF NOT EXISTS optable_partnership;
CREATE SCHEMA IF NOT EXISTS optable_partnership.public;
CREATE SCHEMA IF NOT EXISTS optable_partnership.internal_schema;
CREATE OR REPLACE WAREHOUSE optable_partnership_setup warehouse_size=xsmall;
USE warehouse optable_partnership_setup;

set dcn_slug = 'bd1';
set dcn_account_id = 'JS73429';

CREATE TABLE IF NOT EXISTS optable_partnership.public.dcn_account(dcn_account_id VARCHAR);
DELETE FROM optable_partnership.public.dcn_account;
INSERT INTO optable_partnership.public.dcn_account VALUES($dcn_account_id);
CREATE TABLE IF NOT EXISTS optable_partnership.public.dcn_partners(dcn_slug VARCHAR NOT NULL, snowflake_partner_role VARCHAR NOT NULL);

CREATE OR REPLACE PROCEDURE optable_partnership.public.disconnect_partner(current_dcn_slug VARCHAR, current_dcn_account_id VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  var snowflake_partner_source_share = "snowflake_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_DCN_ACCOUNT_ID + "_source_share";
  var snowflake_partner_dcr_share = "snowflake_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_DCN_ACCOUNT_ID + "_dcr_share";
  var snowflake_partner_source_db = "snowflake_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_DCN_ACCOUNT_ID + "_source_db";
  var snowflake_partner_dcr_db = "snowflake_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_DCN_ACCOUNT_ID + "_dcr_db";
  var snowflake_partner_role = "snowflake_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_DCN_ACCOUNT_ID + "_role";
  var snowflake_partner_warehouse = "snowflake_partner_" + CURRENT_DCN_SLUG + "_" + CURRENT_DCN_ACCOUNT_ID + "_warehouse";

  var statements = [
    "USE ROLE accountadmin",
    "DROP SHARE IF EXISTS " + snowflake_partner_source_share,
    "DROP SHARE IF EXISTS " + snowflake_partner_dcr_share,
    "DROP DATABASE IF EXISTS " + snowflake_partner_source_db,
    "DROP DATABASE IF EXISTS " + snowflake_partner_dcr_db,
    "DROP ROLE IF EXISTS " + snowflake_partner_role,
    "DROP WAREHOUSE IF EXISTS " + snowflake_partner_warehouse,
    "DELETE FROM optable_partnership.public.dcn_partners WHERE dcn_slug ILIKE '" + CURRENT_DCN_SLUG + "'"
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

CREATE OR REPLACE PROCEDURE optable_partnership.public.list_partners()
RETURNS TABLE(dcn_account_id VARCHAR, snowflake_partner_role VARCHAR)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    QUERY STRING;
    res resultset;
BEGIN
   QUERY := 'SELECT * FROM optable_partnership.public.dcn_partners';
   res := (EXECUTE IMMEDIATE :QUERY);
   return table(res);
END;
$$;

CREATE OR REPLACE PROCEDURE optable_partnership.public.validate_query(current_dcn_slug VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  // VALIDATE_QUERY - Michael Rainey and Rachel Blum
  // Adapted for Quickstart by Craig Warman
  // Snowflake Computing, MAR 2022
  //
  // This stored procedure validates a query submitted to the QUERY_REQUESTS table in a
  // simple two-party Snowflake Data Clean Room (DCR) deployment.   It is provided for
  // illustrative purposes only as part of the "Build A Data Clean Room in Snowflake"
  // Quickstart lab, and MUST NOT be used in a production environment.
  //

  try {
    // Set up local variables
    var current_account_stmt = snowflake.createStatement( {sqlText: "SELECT current_account()"} );
    var current_account_result = current_account_stmt.execute();
    current_account_result.next();
    var snowflake_account_id = current_account_result.getColumnValue(1);

    var dcn_account_stmt = snowflake.createStatement( {sqlText: "SELECT dcn_account_id FROM optable_partnership.public.dcn_account LIMIT 1"} );
    var dcn_account_result = dcn_account_stmt.execute();
    dcn_account_result.next();
    var dcn_account_id = dcn_account_result.getColumnValue(1);

    var source_db_name = "snowflake_partner_" + CURRENT_DCN_SLUG + "_" + dcn_account_id + "_source_db";
    var dcr_db_internal_schema_name = "snowflake_partner_" + CURRENT_DCN_SLUG + "_" + dcn_account_id + "_dcr_db.internal_schema";
    var dcr_db_shared_schema_name = "snowflake_partner_" + CURRENT_DCN_SLUG + "_" + dcn_account_id + "_dcr_db.shared_schema";

    var set_session_variable_stmt = snowflake.createStatement( {sqlText: "SET snowflake_partner_dcr_internal_schema_new_requests_all = '" + dcr_db_internal_schema_name + "_" + dcn_account_id + ".new_requests_all'"});
    set_session_variable_stmt.execute();

    var set_session_variable_stmt = snowflake.createStatement( {sqlText: "SET snowflake_partner_dcr_internal_schema_dcn_partner_new_requests = '" + dcr_db_internal_schema_name + ".dcn_partner_new_requests'"});
    set_session_variable_stmt.execute();


    var minimum_record_fetch_threshold = 3;
    var completion_msg = "Finished query validation.";

    // Get parameters
    var account_name = CURRENT_DCN_SLUG.toUpperCase();

    // Create a temporary table to store the most recent query request(s)
    // The tempoary table name is generated using a UUID to ensure uniqueness.
    // First, fetch a UUID string...
    var UUID_sql = "SELECT replace(UUID_STRING(),'-','_');";
    var UUID_statement = snowflake.createStatement( {sqlText: UUID_sql} );
    var UUID_result = UUID_statement.execute();
    UUID_result.next();
    var UUID_str = UUID_result.getColumnValue(1);

    // Next, create the temporary table...
    // Note that its name incorporates the UUID fetched above
    var temp_table_name = dcr_db_internal_schema_name + ".requests_temp_" + UUID_str;
    var create_temp_table_sql = "CREATE OR REPLACE TEMPORARY TABLE " + temp_table_name + " ( \
                                   request_id VARCHAR, match_attempt_id VARCHAR, at_timestamp VARCHAR, \
                                   target_table_name VARCHAR, query_template_name VARCHAR);";
    var create_temp_table_statement = snowflake.createStatement( {sqlText: create_temp_table_sql} );
    var create_temp_table_results = create_temp_table_statement.execute();

    // Finally, insert the most recent query requests into this tempoary table.
    // Note that records are fetched from the NEW_REQUESTS_ALL view, which is built on a Table Stream object.
    // This will cause the Table Stream's offset to be moved forward since a committed DML operation takes place here.
    var insert_temp_table_sql = "INSERT INTO " + temp_table_name + " \
                                 SELECT request_id, match_attempt_id, at_timestamp, target_table_name, query_template_name \
                                 FROM " + dcr_db_internal_schema_name + ".new_requests_all;";
    var insert_temp_table_statement = snowflake.createStatement( {sqlText: insert_temp_table_sql} );
    var insert_temp_table_results = insert_temp_table_statement.execute();

    // We're now ready to fetch query requests from that temporary table.
    var query_requests_sql = "SELECT request_id, match_attempt_id, at_timestamp::string, target_table_name, query_template_name \
                        FROM " + temp_table_name + ";";
    var query_requests_statement = snowflake.createStatement( {sqlText: query_requests_sql} );
    var query_requests_result = query_requests_statement.execute();

    // This loop will iterate once for each query request.
    while (query_requests_result.next()) {
      var timestamp_validated = false;
      var query_template_validated = false;
      var approved_query_text = "NULL";
      var comments = "DECLINED";
      var request_status = "DECLINED";

      var request_id = query_requests_result.getColumnValue(1);
      var match_attempt_id = query_requests_result.getColumnValue(2);
      var at_timestamp = query_requests_result.getColumnValue(3);
      var target_table_name = query_requests_result.getColumnValue(4);
      var query_template_name = query_requests_result.getColumnValue(5);

      // Validate the AT_TIMESTAMP for this query request.
      // Note that it must specify a timestamp from the past.
      try {
        var timestamp_sql = "SELECT CASE (to_timestamp('" + at_timestamp + "') < current_timestamp) WHEN TRUE THEN 'Valid' ELSE 'Not Valid' END;"
        var timestamp_statement = snowflake.createStatement( {sqlText: timestamp_sql} );
        var timestamp_result = timestamp_statement.execute();
        timestamp_result.next();
        timestamp_validated = (timestamp_result.getColumnValue(1) == "Valid");
        if (!timestamp_validated) {
          comments = "DECLINED because AT_TIMESTAMP must specify a timestamp from the past.";
          }
      }
      catch (err) {
        timestamp_validated = false;
        comments = "DECLINED because AT_TIMESTAMP is not valid - Error message from Snowflake DB: " + err.message;
      } // Timestamp validation work ends here.

      if (timestamp_validated) {
      // Fetch the template requested for the query.
      var query_template_sql = "SELECT query_template_text FROM " + dcr_db_shared_schema_name + ".query_templates \
                  WHERE UPPER(query_template_name) = '" + query_template_name.toUpperCase() + "' LIMIT 1;";
      var query_template_statement = snowflake.createStatement( {sqlText: query_template_sql} );
      var query_template_result = query_template_statement.execute();
        query_template_result.next();
        var query_text = query_template_result.getColumnValue(1);

        query_template_validated = (query_text);

        if (!query_template_validated) {
          comments = "DECLINED because query template \"" + query_template_name + "\" does not exist.";}
        else {
          // At this point all validations are complete and the query can be approved.
          request_status = "APPROVED";
          comments = "APPROVED";

          // First, build the approved query from the template as a CTAS...
          approved_query_text = "CREATE OR REPLACE TABLE " + match_attempt_id + "_" + target_table_name + " AS " + query_text;
          approved_query_text = approved_query_text.replace(/@match_attempt_id/g, match_attempt_id);
          approved_query_text = approved_query_text.replace(/@threshold/g, minimum_record_fetch_threshold);
          approved_query_text = approved_query_text.replace(/@attimestamp/g, at_timestamp);
          approved_query_text = approved_query_text.replace(/@snowflake_partner_source_source_schema_profiles/g, "snowflake_partner_" + CURRENT_DCN_SLUG + "_" + dcn_account_id + "_source_db.source_schema.profiles");
          approved_query_text = approved_query_text.replace(/@dcn_partner_source_source_schema_profiles/g, "dcn_partner_" + CURRENT_DCN_SLUG + "_" + snowflake_account_id + "_source_db.source_schema.profiles");
          approved_query_text = approved_query_text.replace(/@dcn_partner_source_information_schema_tables/g, "dcn_partner_" + CURRENT_DCN_SLUG + "_" + snowflake_account_id + "_source_db.information_schema.tables");
          approved_query_text = String.fromCharCode(13, 36, 36) + approved_query_text + String.fromCharCode(13, 36, 36);  // Wrap the query text so that it can be passed to below SQL statements


          // Next, check to see if the approved query already exists in the internal schema APPROVED_QUERY_REQUESTS table...
          var approved_query_exists_sql = "SELECT count(*) FROM " + dcr_db_internal_schema_name + ".approved_query_requests \
                                           WHERE query_text = " + approved_query_text + ";";
      var approved_query_exists_statement = snowflake.createStatement( {sqlText: approved_query_exists_sql} );
      var approved_query_exists_result = approved_query_exists_statement.execute();
      approved_query_exists_result.next();
      var approved_query_found = approved_query_exists_result.getColumnValue(1);

          // Finally, insert the approved query into the internal schema APPROVED_QUERY_REQUESTS table if it doesn't already exist there.
          if (approved_query_found == "0") {
        var insert_approved_query_sql = "INSERT INTO " + dcr_db_internal_schema_name + ".approved_query_requests (query_name, query_text) \
                         VALUES ('" + query_template_name + "', " + approved_query_text + ");";
        var insert_approved_query_statement = snowflake.createStatement( {sqlText: insert_approved_query_sql} );
        var insert_approved_query_result = insert_approved_query_statement.execute();
            }
        }
      } // Template work ends here.

    // Insert an acknowledgment record into the shared schema request_status table for the current query request.
    var request_status_sql = "INSERT INTO " + dcr_db_shared_schema_name + ".request_status \
                  (request_id, request_status, target_table_name, query_text, request_status_ts, comments, account_name) \
                  VALUES (\
                  '" + request_id + "', \
                  '" + request_status + "', \
                  '" + target_table_name + "',\
                  " + approved_query_text + ", \
                  CURRENT_TIMESTAMP(),\
                  '" + comments + "',\
                  '" + account_name + "');";
    var request_status_statement = snowflake.createStatement( {sqlText: request_status_sql} );
    var request_status_result = request_status_statement.execute();

    } // Query request loop ends here.
  }
  catch (err) {
    var result = "Failed: Code: " + err.code + "\n  State: " + err.state;
    result += "\n  Message: " + err.message;
    result += "\nStack Trace:\n" + err.stackTraceTxt;
    return result;
  }
  return completion_msg;
$$
;

CREATE OR REPLACE PROCEDURE optable_partnership.internal_schema.create_rap(snowflake_partner_role VARCHAR, dcr_rap VARCHAR, approved_query_requests VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  var statements = [
    "USE ROLE " + SNOWFLAKE_PARTNER_ROLE,
    "CREATE OR REPLACE ROW ACCESS POLICY " + DCR_RAP + " AS (identifier VARCHAR, match_attempt_id VARCHAR) returns boolean ->" +
        "current_role() IN ('ACCOUNTADMIN', UPPER('" + SNOWFLAKE_PARTNER_ROLE + "'))" +
        "OR EXISTS  (select query_text FROM " + APPROVED_QUERY_REQUESTS + " WHERE query_text=current_statement() OR query_text=sha2(current_statement()));"
  ];
  try {
    for (const stmt of statements) {
      var sql = snowflake.createStatement( {sqlText: stmt} );
      sql.execute();
    }
    return "RAP created";
  } catch (err) {
    var result =  "Failed: Code: " + err.code + "\n  State: " + err.state;
    result += "\n  Message: " + err.message;
    result += "\nStack Trace:\n" + err.stackTraceTxt;
    return result;
  }
$$
;

call optable_partnership.public.disconnect_partner($dcn_slug, $dcn_account_id);

CREATE OR REPLACE PROCEDURE optable_partnership.public.partner_connect(dcn_slug VARCHAR, dcn_account_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
  let snowflake_partner_account_id VARCHAR := current_account();
  let snowflake_partner_username VARCHAR := current_user();
  let snowflake_partner_role VARCHAR := 'snowflake_partner_' || :dcn_slug || '_' || :dcn_account_id || '_role';
  let snowflake_partner_warehouse VARCHAR := 'snowflake_partner_' || :dcn_slug || '_' || :dcn_account_id || '_warehouse';
  let snowflake_partner_source_db VARCHAR := 'snowflake_partner_' || :dcn_slug || '_' || :dcn_account_id || '_source_db';
  let snowflake_partner_dcr_db VARCHAR := 'snowflake_partner_' || :dcn_slug || '_' || :dcn_account_id || '_dcr_db';
  let snowflake_partner_source_share VARCHAR := 'snowflake_partner_' || :dcn_slug || '_' || :dcn_account_id || '_source_share';
  let snowflake_partner_dcr_share VARCHAR := 'snowflake_partner_' || :dcn_slug || '_' || :dcn_account_id || '_dcr_share';
  let snowflake_partner_source_schema VARCHAR := :snowflake_partner_source_db || '.source_schema';
  let snowflake_partner_source_schema_profiles VARCHAR := :snowflake_partner_source_schema || '.profiles';
  let snowflake_partner_dcr_shared_schema VARCHAR := :snowflake_partner_dcr_db || '.shared_schema';
  let snowflake_partner_dcr_shared_schema_query_templates VARCHAR := :snowflake_partner_dcr_shared_schema || '.query_templates';
  let snowflake_partner_dcr_shared_schema_match_requests VARCHAR := :snowflake_partner_dcr_shared_schema || '.match_requests';
  let snowflake_partner_dcr_shared_schema_request_status VARCHAR := :snowflake_partner_dcr_shared_schema || '.request_status';
  let snowflake_partner_dcr_internal_schema VARCHAR := :snowflake_partner_dcr_db || '.internal_schema';
  let snowflake_partner_dcr_internal_schema_approved_query_requests VARCHAR := :snowflake_partner_dcr_internal_schema || '.approved_query_requests';
  let snowflake_partner_dcr_internal_schema_match_attempts VARCHAR := :snowflake_partner_dcr_internal_schema || '.match_attempts';
  let snowflake_partner_source_schema_dcr_rap VARCHAR := :snowflake_partner_source_schema || '.dcr_rap';
  let snowflake_partner_dcr_internal_schema_dcn_partner_new_requests VARCHAR := :snowflake_partner_dcr_internal_schema || '.dcn_partner_new_requests';
  let snowflake_partner_dcr_internal_schema_new_requests_all VARCHAR := :snowflake_partner_dcr_internal_schema || '.new_requests_all';
  let dcn_partner_dcr_share VARCHAR := :dcn_account_id || '.dcn_partner_' || :dcn_slug || '_' || :snowflake_partner_account_id || '_dcr_share';
  let dcn_partner_dcr_db VARCHAR := 'dcn_partner_' || :dcn_slug || '_' || :snowflake_partner_account_id || '_dcr_db';
  let dcn_partner_dcr_shared_schema_query_requests VARCHAR := :dcn_partner_dcr_db || '.shared_schema.query_requests';

  -- Create roles

  USE ROLE securityadmin;
  CREATE OR REPLACE ROLE identifier(:snowflake_partner_role);
  GRANT ROLE identifier(:snowflake_partner_role) TO ROLE sysadmin;
  GRANT ROLE identifier(:snowflake_partner_role) TO USER identifier(:snowflake_partner_username);

  -- Grant privileges to roles
  USE ROLE accountadmin;
  GRANT CREATE DATABASE ON ACCOUNT TO ROLE identifier(:snowflake_partner_role);
  GRANT CREATE SHARE ON ACCOUNT TO ROLE identifier(:snowflake_partner_role);
  GRANT IMPORT SHARE ON ACCOUNT TO ROLE identifier(:snowflake_partner_role);
  GRANT EXECUTE TASK ON ACCOUNT TO ROLE identifier(:snowflake_partner_role);
  GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE identifier(:snowflake_partner_role);

  -- Create virtual warehouse
  USE ROLE identifier(:snowflake_partner_role);
  CREATE OR REPLACE WAREHOUSE identifier(:snowflake_partner_warehouse) warehouse_size=xsmall;
  USE WAREHOUSE identifier(:snowflake_partner_warehouse);

  -- Create source database and schema, along with customer table populated with synthetic data
  -- Note that this dataset has demographics included
  CREATE OR REPLACE DATABASE identifier(:snowflake_partner_source_db);
  CREATE OR REPLACE SCHEMA identifier(:snowflake_partner_source_schema);

  CREATE OR REPLACE TABLE identifier(:snowflake_partner_source_schema_profiles)
  (
    identifier VARCHAR NOT NULL,
    match_attempt_id VARCHAR NOT NULL
  );

  -- Create clean room database
  CREATE OR REPLACE DATABASE identifier(:snowflake_partner_dcr_db);

  -- Create clean room shared schema and objects
  CREATE OR REPLACE SCHEMA identifier(:snowflake_partner_dcr_shared_schema);

  -- Create and populate query template table
  CREATE OR REPLACE TABLE identifier(:snowflake_partner_dcr_shared_schema_query_templates)
  (
    query_template_name VARCHAR,
    query_template_text VARCHAR
  );

  DELETE FROM identifier(:snowflake_partner_dcr_shared_schema_query_templates);  -- Run this if you change any of the below queries
  INSERT INTO identifier(:snowflake_partner_dcr_shared_schema_query_templates)
  VALUES ('match_attempt', $$SELECT dcn_partner.* FROM @dcn_partner_source_source_schema_profiles at(timestamp=>'@attimestamp'::timestamp_tz) dcn_partner
  INNER JOIN @snowflake_partner_source_source_schema_profiles snowflake_partner
  ON dcn_partner.identifier = snowflake_partner.identifier
  WHERE snowflake_partner.match_attempt_id = '@match_attempt_id'
  AND exists (SELECT table_name FROM @dcn_partner_source_information_schema_tables WHERE table_schema = 'SOURCE_SCHEMA' AND table_name = 'PROFILES' AND table_type = 'BASE TABLE');$$);


  -- Create and populate available values table
  CREATE OR REPLACE TABLE identifier(:snowflake_partner_dcr_shared_schema_match_requests)
  (
    match_id VARCHAR,
    match_attempt_id VARCHAR,
    create_time TIMESTAMP
  );

  -- Create request status table
  CREATE OR REPLACE TABLE identifier(:snowflake_partner_dcr_shared_schema_request_status)
  (
    request_id VARCHAR,
    request_status VARCHAR,
    target_table_name VARCHAR,
    query_text VARCHAR,
    request_status_ts TIMESTAMP_NTZ,
    comments VARCHAR,
    account_name VARCHAR
  );

  -- Create clean room internal schema and objects
  CREATE OR REPLACE SCHEMA identifier(:snowflake_partner_dcr_internal_schema);

  -- Create approved query requests table
  CREATE OR REPLACE TABLE identifier(:snowflake_partner_dcr_internal_schema_approved_query_requests)
  (
    query_name VARCHAR,
    query_text VARCHAR
  );

  CREATE OR REPLACE TABLE identifier(:snowflake_partner_dcr_internal_schema_match_attempts)
  (
    match_attempt_id VARCHAR,
    match_result VARCHAR
  );

  USE ROLE accountadmin;
  -- Create and apply row access policy to profiles source table
  call optable_partnership.internal_schema.create_rap(:snowflake_partner_role, :snowflake_partner_source_schema_dcr_rap, :snowflake_partner_dcr_internal_schema_approved_query_requests);

  USE ROLE identifier(:snowflake_partner_role);
  ALTER TABLE identifier(:snowflake_partner_source_schema_profiles) add row access policy identifier(:snowflake_partner_source_schema_dcr_rap) on (identifier, match_attempt_id);

  let share_stmts ARRAY := [
    -- Create outbound shares
    'CREATE OR REPLACE SHARE ' || :snowflake_partner_dcr_share,
    'CREATE OR REPLACE SHARE ' || :snowflake_partner_source_share,
    -- Grant object privileges to DCR share
    'GRANT USAGE ON DATABASE ' || :snowflake_partner_dcr_db || ' TO SHARE ' || :snowflake_partner_dcr_share,
    'GRANT USAGE ON SCHEMA ' || :snowflake_partner_dcr_shared_schema || ' TO SHARE ' || :snowflake_partner_dcr_share,
    'GRANT SELECT ON TABLE ' || :snowflake_partner_dcr_shared_schema_query_templates || ' TO SHARE ' || :snowflake_partner_dcr_share,
    'GRANT SELECT ON TABLE ' || :snowflake_partner_dcr_shared_schema_match_requests || ' TO SHARE ' || :snowflake_partner_dcr_share,
    'GRANT SELECT ON TABLE ' || :snowflake_partner_dcr_shared_schema_request_status || ' TO SHARE ' || :snowflake_partner_dcr_share,

    -- Grant object privileges to source share
    'GRANT USAGE ON DATABASE ' || :snowflake_partner_source_db || ' TO SHARE ' || :snowflake_partner_source_share,
    'GRANT USAGE ON SCHEMA ' || :snowflake_partner_source_schema || ' TO SHARE ' || :snowflake_partner_source_share,
    'GRANT SELECT ON TABLE ' || :snowflake_partner_source_schema_profiles || ' TO SHARE ' ||  :snowflake_partner_source_share,

    -- Add account to shares
    -- Note use of SHARE_RESTRICTIONS clause to enable sharing between Business Critical and Enterprise account deployments
    'USE ROLE accountadmin',
    'ALTER SHARE ' || :snowflake_partner_dcr_share || ' ADD ACCOUNTS = ' || :dcn_account_id || ' SHARE_RESTRICTIONS=false',
    'ALTER SHARE ' || :snowflake_partner_source_share || ' ADD ACCOUNTS = ' || :dcn_account_id || ' SHARE_RESTRICTIONS=false',

    -- Create databases from incoming Party2 share and grant privileges
    'CREATE OR REPLACE DATABASE ' || :dcn_partner_dcr_db || ' FROM SHARE ' || :dcn_partner_dcr_share,
    'GRANT IMPORTED PRIVILEGES ON DATABASE ' || :dcn_partner_dcr_db || ' TO ROLE ' || :snowflake_partner_role
 ];

 FOR i IN 1 TO array_size(:share_stmts) DO
   EXECUTE IMMEDIATE replace(:share_stmts[i-1], '"', '');
 END FOR;

  -- Create Table Stream on shared query requests table
  USE ROLE identifier(:snowflake_partner_role);
  CREATE OR REPLACE STREAM identifier(:snowflake_partner_dcr_internal_schema_dcn_partner_new_requests)
  ON TABLE identifier(:dcn_partner_dcr_shared_schema_query_requests)
    APPEND_ONLY = TRUE
    DATA_RETENTION_TIME_IN_DAYS = 14;

  -- Create view to pull data from the just-created table stream
  CREATE OR REPLACE VIEW identifier(:snowflake_partner_dcr_internal_schema_new_requests_all)
  AS
  SELECT * FROM
      (SELECT request_id,
          match_attempt_id,
          at_timestamp,
          target_table_name,
          query_template_name,
          RANK() OVER (PARTITION BY request_id ORDER BY request_ts DESC) AS current_flag
        FROM identifier(:snowflake_partner_dcr_internal_schema_dcn_partner_new_requests)
        WHERE METADATA$ACTION = 'INSERT'
        ) a
    WHERE a.current_flag = 1
  ;

  USE ROLE accountadmin;

  INSERT INTO optable_partnership.public.dcn_partners (dcn_slug, snowflake_partner_role) VALUES (:dcn_slug, :snowflake_partner_role);

  RETURN 'Partner ' || :dcn_slug || ' is successfully connected.';
END;
;
