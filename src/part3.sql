CREATE ROLE Administrator WITH LOGIN;
GRANT ALL PRIVILEGES ON DATABASE retailanalytics_v1 TO Administrator;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO Administrator;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO Administrator;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO Administrator;
----------------------------------------------------------------------------
----------------------------------------------------------------------------
CREATE ROLE Visitor WITH LOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO Visitor;
----------------------------------------------------------------------------
SET ROLE visitor;
SELECT * FROM analysis_formation_date;

SELECT "current_user"();

INSERT INTO analysis_formation_date VALUES (NOW());

RESET ROLE;
----------------------------------------------------------------------------
SET ROLE administrator;
SELECT * FROM analysis_formation_date;

SELECT "current_user"();

INSERT INTO analysis_formation_date VALUES (NOW());

DELETE FROM analysis_formation_date
WHERE analysis_formation = (SELECT MAX(analysis_formation) FROM analysis_formation_date);

RESET ROLE;