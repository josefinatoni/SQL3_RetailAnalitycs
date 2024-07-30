DROP DATABASE IF EXISTS RetailAnalytics_v1;

DROP TABLE IF EXISTS personal_info CASCADE;
DROP TABLE IF EXISTS cards CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS checks CASCADE;
DROP TABLE IF EXISTS sku_group CASCADE;
DROP TABLE IF EXISTS product_grid CASCADE;
DROP TABLE IF EXISTS stores CASCADE;
DROP TABLE IF EXISTS analysis_formation_date CASCADE;

DROP PROCEDURE IF EXISTS importdata(table_name varchar, file_path varchar, delimiter varchar, header boolean);
DROP PROCEDURE IF EXISTS exportdata(table_name varchar, file_path varchar, delimiter varchar, header boolean);

TRUNCATE personal_info CASCADE;

DROP VIEW IF EXISTS customers;
DROP VIEW IF EXISTS groups;
DROP VIEW IF EXISTS purchase_history;
DROP VIEW IF EXISTS periods;

REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM visitor;
DROP ROLE visitor;

REVOKE ALL PRIVILEGES ON DATABASE retailanalytics_v1 FROM administrator;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM Administrator;
REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM Administrator;
REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM Administrator;
DROP ROLE administrator;