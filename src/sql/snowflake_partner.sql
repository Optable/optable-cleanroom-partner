USE ROLE accountadmin;
CREATE DATABASE IF NOT EXISTS optable_partnership;
CREATE SCHEMA IF NOT EXISTS optable_partnership.public;
CREATE SCHEMA IF NOT EXISTS optable_partnership.internal_schema;
CREATE OR REPLACE WAREHOUSE optable_partnership_setup warehouse_size=xsmall;
USE WAREHOUSE optable_partnership_setup;
CREATE TABLE IF NOT EXISTS optable_partnership.public.dcn_partners(org VARCHAR NOT NULL, partnership_slug VARCHAR NOT NULL, dcn_account_locator_id VARCHAR NOT NULL, snowflake_partner_role VARCHAR NOT NULL);
CREATE TABLE IF NOT EXISTS optable_partnership.public.version(version VARCHAR NOT NULL);
DELETE FROM optable_partnership.public.version;
INSERT INTO optable_partnership.public.version VALUES ('v0.0.1');

CREATE OR REPLACE PROCEDURE optable_partnership.public.partner_disconnect(current_partnership_slug VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  // Set up local variables
  var current_account_stmt = snowflake.createStatement( {sqlText: "SELECT current_account()"} );
  var current_account_result = current_account_stmt.execute();
  current_account_result.next();
  var snowflake_account_locator_id = current_account_result.getColumnValue(1);

  var dcn_account_stmt = snowflake.createStatement( {sqlText: "SELECT dcn_account_locator_id FROM optable_partnership.public.dcn_partners WHERE partnership_slug ILIKE '" + CURRENT_PARTNERSHIP_SLUG + "' LIMIT 1"} );
  var dcn_account_result = dcn_account_stmt.execute();
  dcn_account_result.next();
  var dcn_account_locator_id = dcn_account_result.getColumnValue(1);

  var snowflake_partner_source_share = "snowflake_partner_" + CURRENT_PARTNERSHIP_SLUG + "_" + snowflake_account_locator_id + "_" + dcn_account_locator_id + "_source_share";
  var snowflake_partner_dcr_share = "snowflake_partner_" + CURRENT_PARTNERSHIP_SLUG + "_" + snowflake_account_locator_id + "_" + dcn_account_locator_id + "_dcr_share";
  var snowflake_partner_source_db = "snowflake_partner_" + CURRENT_PARTNERSHIP_SLUG + "_" + snowflake_account_locator_id + "_" + dcn_account_locator_id + "_source_db";
  var snowflake_partner_dcr_db = "snowflake_partner_" + CURRENT_PARTNERSHIP_SLUG + "_" + snowflake_account_locator_id + "_" + dcn_account_locator_id + "_dcr_db";
  var snowflake_partner_role = "snowflake_partner_" + CURRENT_PARTNERSHIP_SLUG + "_" + snowflake_account_locator_id + "_" + dcn_account_locator_id + "_role";
  var snowflake_partner_warehouse = "snowflake_partner_" + CURRENT_PARTNERSHIP_SLUG + "_" + snowflake_account_locator_id + "_" + dcn_account_locator_id + "_warehouse";
  var dcn_partner_dcr_db = "dcn_partner_" + CURRENT_PARTNERSHIP_SLUG + "_" + snowflake_account_locator_id + "_" + dcn_account_locator_id + "_dcr_db";

  var statements = [
    "USE ROLE accountadmin",
    "DROP SHARE IF EXISTS " + snowflake_partner_source_share,
    "DROP SHARE IF EXISTS " + snowflake_partner_dcr_share,
    "DROP DATABASE IF EXISTS " + snowflake_partner_source_db,
    "DROP DATABASE IF EXISTS " + snowflake_partner_dcr_db,
    "DROP ROLE IF EXISTS " + snowflake_partner_role,
    "DROP DATABASE IF EXISTS " + dcn_partner_dcr_db,
    "DELETE FROM optable_partnership.public.dcn_partners WHERE partnership_slug ILIKE '" + CURRENT_PARTNERSHIP_SLUG + "'"
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
  return 'Partner ' + CURRENT_PARTNERSHIP_SLUG + ' is disconnected';
$$
;


CREATE OR REPLACE PROCEDURE optable_partnership.public.partner_list()
RETURNS TABLE(organization_name VARCHAR, partnership_slug VARCHAR, dcn_account_locator_id VARCHAR, snowflake_partner_role VARCHAR, status VARCHAR)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    QUERY STRING;
    res resultset;
BEGIN
   -- TODO: change the hardcoded connected to actual partnership status
   QUERY := 'SELECT *, \'connected\' FROM optable_partnership.public.dcn_partners';
   res := (EXECUTE IMMEDIATE :QUERY);
   return table(res);
END;
$$
;


CREATE OR REPLACE PROCEDURE optable_partnership.internal_schema.create_rap(snowflake_partner_role VARCHAR, dcr_rap VARCHAR, match_requests VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  var statements = [
    "USE ROLE " + SNOWFLAKE_PARTNER_ROLE,
    "CREATE OR REPLACE ROW ACCESS POLICY " + DCR_RAP + " AS (identifier VARCHAR, match_id VARCHAR) returns boolean ->" +
        "current_role() IN ('ACCOUNTADMIN', UPPER('" + SNOWFLAKE_PARTNER_ROLE + "'))" +
        "OR EXISTS  (select query_text FROM " + MATCH_REQUESTS + " WHERE query_text=current_statement() OR query_text=sha2(current_statement()));"
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


CREATE OR REPLACE PROCEDURE optable_partnership.public.partner_connect(org VARCHAR, partnership_slug VARCHAR, dcn_account_locator_id VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
  let snowflake_partner_account_locator_id VARCHAR := current_account();
  let snowflake_partner_username VARCHAR := current_user();
  let snowflake_partner_role VARCHAR := 'snowflake_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_role';
  let snowflake_partner_warehouse VARCHAR := 'snowflake_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_warehouse';
  let snowflake_partner_source_db VARCHAR := 'snowflake_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_source_db';
  let snowflake_partner_dcr_db VARCHAR := 'snowflake_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_dcr_db';
  let snowflake_partner_source_share VARCHAR := 'snowflake_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_source_share';
  let snowflake_partner_dcr_share VARCHAR := 'snowflake_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_dcr_share';
  let snowflake_partner_source_schema VARCHAR := :snowflake_partner_source_db || '.source_schema';
  let snowflake_partner_source_schema_profiles VARCHAR := :snowflake_partner_source_schema || '.profiles';
  let snowflake_partner_dcr_shared_schema VARCHAR := :snowflake_partner_dcr_db || '.shared_schema';
  let snowflake_partner_dcr_shared_schema_query_templates VARCHAR := :snowflake_partner_dcr_shared_schema || '.query_templates';
  let snowflake_partner_dcr_shared_schema_match_requests VARCHAR := :snowflake_partner_dcr_shared_schema || '.match_requests';
  let snowflake_partner_dcr_internal_schema VARCHAR := :snowflake_partner_dcr_db || '.internal_schema';
  let snowflake_partner_dcr_shared_schema_matches VARCHAR := :snowflake_partner_dcr_shared_schema || '.matches';
  let snowflake_partner_dcr_internal_schema_match_attempts VARCHAR := :snowflake_partner_dcr_internal_schema || '.match_attempts';
  let snowflake_partner_source_schema_dcr_rap VARCHAR := :snowflake_partner_source_schema || '.dcr_rap';
  let dcn_partner_dcr_share VARCHAR := :dcn_account_locator_id || '.dcn_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_dcr_share';
  let dcn_partner_dcr_db VARCHAR := 'dcn_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_dcr_db';
  let dcn_partner_dcr_shared_schema_match_attempts VARCHAR := :dcn_partner_dcr_db || '.shared_schema.match_attempts';

  -- Create roles

  DELETE FROM optable_partnership.public.dcn_partners WHERE partnership_slug ILIKE :partnership_slug;
  INSERT INTO optable_partnership.public.dcn_partners (org, partnership_slug, dcn_account_locator_id, snowflake_partner_role) VALUES (:org, :partnership_slug, :dcn_account_locator_id, :snowflake_partner_role);

  USE ROLE securityadmin;
  CREATE OR REPLACE ROLE identifier(:snowflake_partner_role);
  GRANT ROLE identifier(:snowflake_partner_role) TO ROLE sysadmin;
  GRANT ROLE identifier(:snowflake_partner_role) TO USER identifier(:snowflake_partner_username);

  -- Grant privileges to roles
  USE ROLE accountadmin;
  GRANT CREATE DATABASE ON ACCOUNT TO ROLE identifier(:snowflake_partner_role);
  GRANT CREATE SHARE ON ACCOUNT TO ROLE identifier(:snowflake_partner_role);
  GRANT IMPORT SHARE ON ACCOUNT TO ROLE identifier(:snowflake_partner_role);

  GRANT USAGE ON DATABASE optable_partnership TO ROLE identifier(:snowflake_partner_role);
  GRANT USAGE ON WAREHOUSE optable_partnership_setup TO ROLE identifier(:snowflake_partner_role);
  GRANT USAGE ON SCHEMA optable_partnership.public TO ROLE identifier(:snowflake_partner_role);
  GRANT USAGE ON SCHEMA optable_partnership.internal_schema TO ROLE identifier(:snowflake_partner_role);
  GRANT ALL privileges ON ALL PROCEDURES IN DATABASE optable_partnership TO identifier(:snowflake_partner_role);
  GRANT ALL privileges ON ALL FUNCTIONS IN DATABASE optable_partnership TO identifier(:snowflake_partner_role);
  GRANT SELECT ON ALL TABLES IN DATABASE optable_partnership TO ROLE identifier(:snowflake_partner_role);
  GRANT ALL privileges ON FUTURE PROCEDURES IN DATABASE optable_partnership TO identifier(:snowflake_partner_role);
  GRANT ALL privileges ON FUTURE FUNCTIONS IN DATABASE optable_partnership TO identifier(:snowflake_partner_role);
  GRANT SELECT ON FUTURE TABLES IN DATABASE optable_partnership TO ROLE identifier(:snowflake_partner_role);

  -- Create virtual warehouse
  USE ROLE identifier(:snowflake_partner_role);

  -- Create source database and schema, along with customer table populated with synthetic data
  -- Note that this dataset has demographics included
  CREATE OR REPLACE DATABASE identifier(:snowflake_partner_source_db);
  CREATE OR REPLACE SCHEMA identifier(:snowflake_partner_source_schema);

  CREATE OR REPLACE TABLE identifier(:snowflake_partner_source_schema_profiles)
  (
    identifier VARCHAR NOT NULL,
    match_id VARCHAR NOT NULL,
    request_id VARCHAR NOT NULL
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
  VALUES ('match_attempt', $$
SELECT dcn_partner.identifier AS id FROM @dcn_partner_source_source_schema_profiles dcn_partner
INNER JOIN @snowflake_partner_source_source_schema_profiles snowflake_partner
ON dcn_partner.identifier = snowflake_partner.identifier
WHERE snowflake_partner.match_id = '@match_id'
AND exists (SELECT table_name FROM @dcn_partner_source_information_schema_tables WHERE table_schema = 'SOURCE_SCHEMA' AND table_name = 'PROFILES' AND table_type = 'BASE TABLE');
  $$);

  -- Create request status table
  CREATE OR REPLACE TABLE identifier(:snowflake_partner_dcr_shared_schema_match_requests)
  (
    request_id VARCHAR,
    match_id VARCHAR,
    match_name VARCHAR,
    version VARCHAR,
    target_table_name VARCHAR,
    query_text VARCHAR,
    at_timestamp TIMESTAMP_TZ,
    at_timestamp_text VARCHAR
  );

  -- Create clean room internal schema and objects
  CREATE OR REPLACE SCHEMA identifier(:snowflake_partner_dcr_internal_schema);

  CREATE OR REPLACE TABLE identifier(:snowflake_partner_dcr_shared_schema_matches)
  (
    match_id VARCHAR,
    match_name VARCHAR
  );

  CREATE OR REPLACE TABLE identifier(:snowflake_partner_dcr_internal_schema_match_attempts)
  (
    request_id VARCHAR,
    match_id VARCHAR,
    match_attempt_id VARCHAR,
    match_result VARIANT,
    attempt_ts TIMESTAMP_TZ,
    status VARCHAR
  );

  -- Create and apply row access policy to profiles source table
  call optable_partnership.internal_schema.create_rap(:snowflake_partner_role, :snowflake_partner_source_schema_dcr_rap, :snowflake_partner_dcr_shared_schema_match_requests);

  USE ROLE identifier(:snowflake_partner_role);
  ALTER TABLE identifier(:snowflake_partner_source_schema_profiles) add row access policy identifier(:snowflake_partner_source_schema_dcr_rap) on (identifier, match_id);

  let share_stmts ARRAY := [
    -- Create outbound shares
    'CREATE OR REPLACE SHARE ' || :snowflake_partner_dcr_share,
    'CREATE OR REPLACE SHARE ' || :snowflake_partner_source_share,
    -- Grant object privileges to DCR share
    'GRANT USAGE ON DATABASE ' || :snowflake_partner_dcr_db || ' TO SHARE ' || :snowflake_partner_dcr_share,
    'GRANT USAGE ON SCHEMA ' || :snowflake_partner_dcr_shared_schema || ' TO SHARE ' || :snowflake_partner_dcr_share,
    'GRANT SELECT ON TABLE ' || :snowflake_partner_dcr_shared_schema_query_templates || ' TO SHARE ' || :snowflake_partner_dcr_share,
    'GRANT SELECT ON TABLE ' || :snowflake_partner_dcr_shared_schema_matches || ' TO SHARE ' || :snowflake_partner_dcr_share,
    'GRANT SELECT ON TABLE ' || :snowflake_partner_dcr_shared_schema_match_requests || ' TO SHARE ' || :snowflake_partner_dcr_share,

    -- Grant object privileges to source share
    'GRANT USAGE ON DATABASE ' || :snowflake_partner_source_db || ' TO SHARE ' || :snowflake_partner_source_share,
    'GRANT USAGE ON SCHEMA ' || :snowflake_partner_source_schema || ' TO SHARE ' || :snowflake_partner_source_share,
    'GRANT SELECT ON TABLE ' || :snowflake_partner_source_schema_profiles || ' TO SHARE ' ||  :snowflake_partner_source_share,

    -- Add account to shares
    -- Note use of SHARE_RESTRICTIONS clause to enable sharing between Business Critical and Enterprise account deployments
    'USE ROLE accountadmin',
    'ALTER SHARE ' || :snowflake_partner_dcr_share || ' ADD ACCOUNTS = ' || :dcn_account_locator_id || ' SHARE_RESTRICTIONS=false',
    'ALTER SHARE ' || :snowflake_partner_source_share || ' ADD ACCOUNTS = ' || :dcn_account_locator_id || ' SHARE_RESTRICTIONS=false',

    -- Create databases from incoming Party2 share and grant privileges
    'CREATE OR REPLACE DATABASE ' || :dcn_partner_dcr_db || ' FROM SHARE ' || :dcn_partner_dcr_share,
    'GRANT IMPORTED PRIVILEGES ON DATABASE ' || :dcn_partner_dcr_db || ' TO ROLE ' || :snowflake_partner_role
 ];

 FOR i IN 1 TO array_size(:share_stmts) DO
   EXECUTE IMMEDIATE replace(:share_stmts[i-1], '"', '');
 END FOR;

  -- Create Table Stream on shared query requests table
  USE ROLE accountadmin;

  RETURN 'Partner ' || :partnership_slug || ' is successfully connected.';
  EXCEPTION
    WHEN OTHER THEN
        RETURN 'An error occurred, please make sure that you have entered the correct Account Locator ID, and that you are authorized to call the partner_connect function. Actual error message: ' || sqlerrm;
END;


CREATE OR REPLACE PROCEDURE optable_partnership.public.match_run(partnership_slug VARCHAR, match_id VARCHAR, source_table VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
  let account_res RESULTSET := (SELECT dcn_account_locator_id FROM optable_partnership.public.dcn_partners WHERE partnership_slug ILIKE :partnership_slug LIMIT 1);
  let c1 cursor for account_res;
  let dcn_account_locator_id VARCHAR := 'dummy';
  for row_variable in c1 do
    dcn_account_locator_id := row_variable.dcn_account_locator_id;
  end for;

  let snowflake_partner_account_locator_id VARCHAR := current_account();
  let snowflake_partner_role VARCHAR := 'snowflake_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_role';
  let snowflake_partner_warehouse VARCHAR := 'snowflake_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_warehouse';
  let snowflake_partner_dcr_db VARCHAR := 'snowflake_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_dcr_db';
  let snowflake_partner_dcr_shared_schema VARCHAR := :snowflake_partner_dcr_db || '.shared_schema';
  let snowflake_partner_dcr_shared_schema_matches VARCHAR := :snowflake_partner_dcr_shared_schema || '.matches';
  let snowflake_partner_dcr_shared_schema_match_requests VARCHAR := :snowflake_partner_dcr_shared_schema || '.match_requests';
  let snowflake_partner_dcr_shared_schema_query_templates VARCHAR := :snowflake_partner_dcr_shared_schema || '.query_templates';
  let snowflake_partner_dcr_internal_schema VARCHAR := :snowflake_partner_dcr_db || '.internal_schema';
  let snowflake_partner_dcr_internal_schema_match_attempts VARCHAR := :snowflake_partner_dcr_internal_schema || '.match_attempts';
  let snowflake_partner_source_db VARCHAR := 'snowflake_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_source_db';
  let snowflake_partner_source_schema VARCHAR := :snowflake_partner_source_db || '.source_schema';
  let snowflake_partner_source_schema_profiles VARCHAR := :snowflake_partner_source_schema || '.profiles';
  let dcn_partner_source_db VARCHAR := 'dcn_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_source_db';
  let dcn_partner_source_schema VARCHAR := :dcn_partner_source_db || '.source_schema';
  let dcn_partner_source_schema_profiles VARCHAR := :dcn_partner_source_schema || '.profiles';
  let dcn_partner_information_schema_tables VARCHAR := :dcn_partner_source_db || '.information_schema.tables';
  let target_table_name VARCHAR := REPLACE(dcn_partner_source_schema_profiles || '_' || :match_id, '-', '_');
  let request_id VARCHAR := uuid_string();

  let double_run_exception EXCEPTION := EXCEPTION (-50001, 'You cannot schedule the same match more than once at a time. Match ' || :match_id || ' is already running');
  let not_found_exception EXCEPTION := EXCEPTION (-50002, 'Match ' || :match_id || ' is not found');

  USE ROLE identifier(:snowflake_partner_role);
  USE WAREHOUSE optable_partnership_setup;

  -- Make sure we have access to the source table:
  SELECT COUNT(*) FROM identifier(:source_table);

  call optable_partnership.public.cleanup_profiles(:partnership_slug);

  let matches_id_res RESULTSET := (SELECT * FROM identifier(:snowflake_partner_dcr_shared_schema_matches) WHERE match_id ILIKE :match_id LIMIT 1);
  let match_c1 cursor for matches_id_res;
  let match_is_missing BOOLEAN := true;
  for row_variable in match_c1 do
    match_is_missing := false;
  end for;
  IF (match_is_missing = TRUE) THEN
    RAISE not_found_exception;
  END IF;

  let matches_res RESULTSET := (SELECT * FROM identifier(:snowflake_partner_source_schema_profiles) WHERE match_id ILIKE :match_id LIMIT 1);
  let c2 cursor for matches_res;
  for row_variable in c2 do
    RAISE double_run_exception;
  end for;

  BEGIN TRANSACTION;
  let columns_res RESULTSET := (call optable_partnership.internal_schema.show_columns(:source_table));
  let c3 cursor for columns_res;
  for r in c3 do
    let cn VARCHAR := r.column_name;
    IF (cn ILIKE 'id_e%') THEN
      INSERT INTO identifier(:snowflake_partner_source_schema_profiles) SELECT optable_partnership.internal_schema.parse_email(identifier(:cn)), :match_id, :request_id FROM identifier(:source_table);
    ELSEIF (cn ILIKE 'id_p%') THEN
      INSERT INTO identifier(:snowflake_partner_source_schema_profiles) SELECT optable_partnership.internal_schema.parse_phone(identifier(:cn)), :match_id, :request_id FROM identifier(:source_table);
    ELSEIF (cn ILIKE 'id_i4%') THEN
      INSERT INTO identifier(:snowflake_partner_source_schema_profiles) SELECT optable_partnership.internal_schema.parse_ipv4(identifier(:cn)), :match_id, :request_id FROM identifier(:source_table);
    ELSEIF (cn ILIKE 'id_i6%') THEN
      INSERT INTO identifier(:snowflake_partner_source_schema_profiles) SELECT optable_partnership.internal_schema.parse_ipv6(identifier(:cn)), :match_id, :request_id FROM identifier(:source_table);
    ELSEIF (cn ILIKE 'id_a%') THEN
      INSERT INTO identifier(:snowflake_partner_source_schema_profiles) SELECT optable_partnership.internal_schema.parse_apple(identifier(:cn)), :match_id, :request_id FROM identifier(:source_table);
    ELSEIF (cn ILIKE 'id_g%') THEN
      INSERT INTO identifier(:snowflake_partner_source_schema_profiles) SELECT optable_partnership.internal_schema.parse_google(identifier(:cn)), :match_id, :request_id FROM identifier(:source_table);
    ELSEIF (cn ILIKE 'id_r%') THEN
      INSERT INTO identifier(:snowflake_partner_source_schema_profiles) SELECT optable_partnership.internal_schema.parse_roku(identifier(:cn)), :match_id, :request_id FROM identifier(:source_table);
    ELSEIF (cn ILIKE 'id_s%') THEN
      INSERT INTO identifier(:snowflake_partner_source_schema_profiles) SELECT optable_partnership.internal_schema.parse_samsung(identifier(:cn)), :match_id, :request_id FROM identifier(:source_table);
    ELSEIF (cn ILIKE 'id_f%') THEN
      INSERT INTO identifier(:snowflake_partner_source_schema_profiles) SELECT optable_partnership.internal_schema.parse_amazon(identifier(:cn)), :match_id, :request_id FROM identifier(:source_table);
    ELSEIF (cn ILIKE 'id_n%') THEN
      INSERT INTO identifier(:snowflake_partner_source_schema_profiles) SELECT optable_partnership.internal_schema.parse_net_id(identifier(:cn)), :match_id, :request_id FROM identifier(:source_table);
    ELSEIF (cn ILIKE 'id%') THEN
      INSERT INTO identifier(:snowflake_partner_source_schema_profiles) SELECT optable_partnership.internal_schema.parse_id(identifier(:cn)), :match_id, :request_id FROM identifier(:source_table);
    END IF;
  end for;
  ALTER SESSION SET timezone = 'UTC';

  let version_res RESULTSET := (SELECT version FROM optable_partnership.public.version);
  let c4 cursor for version_res;
  let version VARCHAR := 'dummy';
  for r in c4 do
    version := r.version;
  end for;


  let attempt_ts TIMESTAMP_TZ := current_timestamp();
  let template_res RESULTSET := (SELECT query_template_text FROM identifier(:snowflake_partner_dcr_shared_schema_query_templates) WHERE query_template_name LIKE 'match_attempt' LIMIT 1);
  let c5 cursor for template_res;
  let query_template_text VARCHAR := 'dummy';
  for row_variable in c5 do
    query_template_text := row_variable.query_template_text;
    INSERT INTO identifier(:snowflake_partner_dcr_shared_schema_match_requests)
    SELECT
      :request_id,
      :match_id,
      match_name,
      :version,
      :target_table_name,
      'INSERT INTO ' || :target_table_name || ' ' || REPLACE(
        REPLACE(
          REPLACE(
             REPLACE(:query_template_text,
            '@dcn_partner_source_source_schema_profiles', :dcn_partner_source_schema_profiles),
          '@snowflake_partner_source_source_schema_profiles', :snowflake_partner_source_schema_profiles),
        '@match_id', :match_id),
      '@dcn_partner_source_information_schema_tables', :dcn_partner_information_schema_tables),
      :attempt_ts,
      :attempt_ts FROM identifier(:snowflake_partner_dcr_shared_schema_matches) WHERE match_id ILIKE :match_id;
  end for;
  INSERT INTO identifier(:snowflake_partner_dcr_internal_schema_match_attempts) SELECT :request_id, :match_id, '', parse_json('{}'), :attempt_ts, 'establishing connection';
  COMMIT;

  RETURN 'A match attempt is successfully scheduled';
END;


CREATE OR REPLACE PROCEDURE optable_partnership.public.match_get_results(partnership_slug VARCHAR, match_id VARCHAR)
RETURNS TABLE(match_id VARCHAR, match_run_id VARCHAR, match_result VARIANT, run_time TIMESTAMP_TZ, status VARCHAR)
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
  let snowflake_partner_account_locator_id VARCHAR := current_account();
  let account_res RESULTSET := (SELECT dcn_account_locator_id FROM optable_partnership.public.dcn_partners WHERE partnership_slug ILIKE :partnership_slug LIMIT 1);
  let c1 cursor for account_res;
  let dcn_account_locator_id VARCHAR := '';
  for row_variable in c1 do
    dcn_account_locator_id := row_variable.dcn_account_locator_id;
  end for;

  let dcn_partner_dcr_db VARCHAR := 'dcn_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_dcr_db';
  let dcn_partner_dcr_shared_schema VARCHAR := :dcn_partner_dcr_db || '.shared_schema';
  let dcn_partner_dcr_shared_schema_match_attempts VARCHAR := :dcn_partner_dcr_shared_schema || '.match_attempts';
  let snowflake_partner_dcr_db VARCHAR := 'snowflake_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_dcr_db';
  let snowflake_partner_dcr_internal_schema VARCHAR := :snowflake_partner_dcr_db || '.internal_schema';
  let snowflake_partner_dcr_internal_schema_match_attempts VARCHAR := :snowflake_partner_dcr_internal_schema || '.match_attempts';
  let res RESULTSET := (
    WITH dcn AS (
        SELECT match_id, request_id, match_result, attempt_ts, status
        FROM identifier(:dcn_partner_dcr_shared_schema_match_attempts)
        WHERE match_id ILIKE :match_id
    ),
    snowflake AS (
        SELECT match_id, request_id, match_result, attempt_ts, status
        FROM identifier(:snowflake_partner_dcr_internal_schema_match_attempts)
        WHERE match_id ILIKE :match_id
        AND request_id NOT IN (SELECT request_id FROM dcn)
    )
    SELECT * FROM dcn
    UNION
    SELECT * FROM snowflake
    ORDER BY attempt_ts DESC
  );
  return table(res);
END;


CREATE OR REPLACE PROCEDURE optable_partnership.public.cleanup_profiles(partnership_slug VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
  let account_res RESULTSET := (SELECT dcn_account_locator_id FROM optable_partnership.public.dcn_partners WHERE partnership_slug ILIKE :partnership_slug LIMIT 1);
  let c1 cursor for account_res;
  let dcn_account_locator_id VARCHAR := 'dummy';
  for row_variable in c1 do
    dcn_account_locator_id := row_variable.dcn_account_locator_id;
  end for;

  let snowflake_partner_account_locator_id VARCHAR := current_account();
  let snowflake_partner_dcr_db VARCHAR := 'snowflake_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_dcr_db';
  let snowflake_partner_dcr_shared_schema VARCHAR := :snowflake_partner_dcr_db || '.shared_schema';
  let dcn_partner_dcr_db VARCHAR := 'dcn_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_dcr_db';
  let dcn_partner_dcr_shared_schema_match_attempts VARCHAR := :dcn_partner_dcr_db || '.shared_schema.match_attempts';
  let snowflake_partner_source_db VARCHAR := 'snowflake_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_source_db';
  let snowflake_partner_source_schema VARCHAR := :snowflake_partner_source_db || '.source_schema';
  let snowflake_partner_source_schema_profiles VARCHAR := :snowflake_partner_source_schema || '.profiles';

  DELETE FROM identifier(:snowflake_partner_source_schema_profiles) WHERE LOWER(request_id) IN (
    SELECT LOWER(request_id) FROM identifier(:dcn_partner_dcr_shared_schema_match_attempts) WHERE status IN ('errored', 'completed')
  );

END;


CREATE OR REPLACE PROCEDURE optable_partnership.public.version()
RETURNS TABLE(version VARCHAR)
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
  let res RESULTSET := (SELECT version FROM optable_partnership.public.version);
  return table(res);
END;


CREATE OR REPLACE FUNCTION optable_partnership.internal_schema.parse_email(id VARCHAR)
RETURNS VARCHAR
AS
$$
  CASE
    WHEN LENGTH(REPLACE(id, 'e:', '')) <> 64 THEN
      'e:'|| SHA2(REPLACE(TRIM(id), 'e:', ''), 256)
    WHEN TRY_HEX_DECODE_BINARY(id) IS NULL THEN
      'e:'|| SHA2(TRIM(id), 256)
    ELSE 'e:' || id
  END
$$
;


CREATE OR REPLACE FUNCTION optable_partnership.internal_schema.parse_apple(id VARCHAR)
RETURNS VARCHAR
AS
$$
  'a:' || id
$$
;


CREATE OR REPLACE FUNCTION optable_partnership.internal_schema.parse_google(id VARCHAR)
RETURNS VARCHAR
AS
$$
  'g:' || id
$$
;


CREATE OR REPLACE FUNCTION optable_partnership.internal_schema.parse_ipv4(id VARCHAR)
RETURNS VARCHAR
AS
$$
  'i4:' || id
$$
;


CREATE OR REPLACE FUNCTION optable_partnership.internal_schema.parse_ipv6(id VARCHAR)
RETURNS VARCHAR
AS
$$
  'i6:' || id
$$
;


CREATE OR REPLACE FUNCTION optable_partnership.internal_schema.parse_samsung(id VARCHAR)
RETURNS VARCHAR
AS
$$
  's:' || id
$$
;


CREATE OR REPLACE FUNCTION optable_partnership.internal_schema.parse_roku(id VARCHAR)
RETURNS VARCHAR
AS
$$
  'r:' || id
$$
;


CREATE OR REPLACE FUNCTION optable_partnership.internal_schema.parse_amazon(id VARCHAR)
RETURNS VARCHAR
AS
$$
  'f:' || id
$$
;


CREATE OR REPLACE FUNCTION optable_partnership.internal_schema.parse_phone(id VARCHAR)
RETURNS VARCHAR
AS
$$
  CASE
    WHEN LENGTH(REPLACE(id, 'p:', '')) <> 64 THEN
      'p:'|| SHA2(REPLACE(TRIM(id), 'p:', ''), 256)
    WHEN TRY_HEX_DECODE_BINARY(id) IS NULL THEN
      'p:'|| SHA2(TRIM(id), 256)
    ELSE 'p:' || id
  END
$$
;


CREATE OR REPLACE FUNCTION optable_partnership.internal_schema.parse_net_id(id VARCHAR)
RETURNS VARCHAR
AS
$$
  'n:' || id
$$
;


CREATE OR REPLACE FUNCTION optable_partnership.internal_schema.parse_id(id VARCHAR)
RETURNS VARCHAR
AS
$$
  CASE
    WHEN STARTSWITH(id, 'e') OR STARTSWITH(id, 'a') OR STARTSWITH(id, 'g') OR STARTSWITH(id, 'i4') OR STARTSWITH(id, 'i6') OR STARTSWITH(id, 's') OR STARTSWITH(id, 'r') OR STARTSWITH(id, 'f') OR STARTSWITH(id, 'p') OR STARTSWITH(id, 'n') THEN
      CASE
        WHEN STARTSWITH(id, 'e:') THEN
          CASE
            WHEN LENGTH(REPLACE(id, 'e:', '')) <> 64 THEN
              'e:'|| SHA2(REPLACE(TRIM(id), 'e:', ''), 256)
            WHEN TRY_HEX_DECODE_BINARY(REPLACE(id, 'e:', '')) IS NULL THEN
              'e:'|| SHA2(REPLACE(TRIM(id), 'e:', ''), 256)
            ELSE id
          END
        WHEN STARTSWITH(id, 'p:') THEN
          CASE
            WHEN LENGTH(REPLACE(id, 'p:', '')) <> 64 THEN
              'p:'|| SHA2(REPLACE(TRIM(id), 'p:', ''), 256)
            WHEN TRY_HEX_DECODE_BINARY(REPLACE(id, 'p:', '')) IS NULL THEN
              'e:'|| SHA2(REPLACE(id, 'p:', ''), 256)
            ELSE id
          END
        ELSE id
      END
    ELSE
      CASE
        WHEN CONTAINS(id, '@') THEN
          'e:'|| SHA2(TRIM(id), 256)
        ELSE 'p:'|| SHA2(TRIM(id), 256)
      END
  END
$$
;


CREATE OR REPLACE PROCEDURE optable_partnership.public.match_create(partnership_slug VARCHAR, match_name VARCHAR)
RETURNS TABLE(match_id VARCHAR)
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
  let snowflake_partner_account_locator_id VARCHAR := current_account();
  let account_res RESULTSET := (SELECT dcn_account_locator_id FROM optable_partnership.public.dcn_partners WHERE partnership_slug ILIKE :partnership_slug LIMIT 1);
  let c1 cursor for account_res;
  let dcn_account_locator_id VARCHAR := 'dummy';
  for row_variable in c1 do
    dcn_account_locator_id := row_variable.dcn_account_locator_id;
  end for;
  let snowflake_partner_dcr_db VARCHAR := 'snowflake_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_dcr_db';
  let snowflake_partner_dcr_shared_schema VARCHAR := :snowflake_partner_dcr_db || '.shared_schema';
  let snowflake_partner_dcr_shared_schema_matches VARCHAR := :snowflake_partner_dcr_shared_schema || '.matches';
  let uuid := uuid_string();
  INSERT INTO identifier(:snowflake_partner_dcr_shared_schema_matches) VALUES (:uuid, :match_name);
  let res RESULTSET := (SELECT match_id FROM identifier(:snowflake_partner_dcr_shared_schema_matches) WHERE match_id ILIKE :uuid);
  return table(res);
END;


CREATE OR REPLACE PROCEDURE optable_partnership.public.match_list(partnership_slug VARCHAR)
RETURNS TABLE(match_id VARCHAR, match_name VARCHAR)
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
  let snowflake_partner_account_locator_id VARCHAR := current_account();
  let account_res RESULTSET := (SELECT dcn_account_locator_id FROM optable_partnership.public.dcn_partners WHERE partnership_slug ILIKE :partnership_slug LIMIT 1);
  let c1 cursor for account_res;
  let dcn_account_locator_id VARCHAR := 'dummy';
  for row_variable in c1 do
    dcn_account_locator_id := row_variable.dcn_account_locator_id;
  end for;
  let snowflake_partner_dcr_db VARCHAR := 'snowflake_partner_' || :partnership_slug || '_' || :snowflake_partner_account_locator_id || '_' || :dcn_account_locator_id || '_dcr_db';
  let snowflake_partner_dcr_shared_schema VARCHAR := :snowflake_partner_dcr_db || '.shared_schema';
  let snowflake_partner_dcr_shared_schema_matches VARCHAR := :snowflake_partner_dcr_shared_schema || '.matches';
  let res RESULTSET := (SELECT * FROM identifier(:snowflake_partner_dcr_shared_schema_matches));
  RETURN table(res);
END;


CREATE OR REPLACE PROCEDURE optable_partnership.internal_schema.show_columns(source_table VARCHAR)
RETURNS TABLE(column_name VARCHAR)
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
  SHOW COLUMNS IN identifier(:source_table);
  let columns_res RESULTSET := (SELECT "column_name" from table(result_scan(last_query_id())));
  return TABLE(columns_res);
END;


CREATE OR REPLACE PROCEDURE optable_partnership.public.grant_permission(partnership_slug VARCHAR, database_name VARCHAR, schema_name VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
  let account_res RESULTSET := (SELECT dcn_account_locator_id FROM optable_partnership.public.dcn_partners WHERE partnership_slug ILIKE :partnership_slug LIMIT 1);
  let c1 cursor for account_res;
  let dcn_account_locator_id VARCHAR := 'dummy';
  for row_variable in c1 do
    dcn_account_locator_id := row_variable.dcn_account_locator_id;
  end for;

  let snowflake_partner_account_locator_id VARCHAR := current_account();

  let role_name VARCHAR := 'snowflake_partner_' || partnership_slug || '_' || snowflake_partner_account_locator_id || '_' || dcn_account_locator_id || '_role';
  let schema_name_full VARCHAR := :database_name || '.' || :schema_name;
  GRANT SELECT ON ALL TABLES IN DATABASE identifier(:database_name) TO ROLE identifier(:role_name);
  GRANT USAGE ON DATABASE identifier(:database_name) TO ROLE identifier(:role_name);
  GRANT USAGE ON SCHEMA identifier(:schema_name_full) TO ROLE identifier(:role_name);
  RETURN 'permissions granted';
END;
