CREATE DATABASE RetailAnalytics_v1;

CREATE TABLE personal_info (
    Customer_ID serial primary key ,
    Customer_Name varchar not null ,
    Customer_Surname varchar not null,
    Customer_Primary_Email varchar(255),
    Customer_Primary_Phone varchar(12)
);

CREATE TABLE cards (
    Customer_Card_ID serial not null primary key,
    Customer_ID bigint,
    constraint fk_cards_customer_id foreign key (Customer_ID) references personal_info(Customer_ID)
);

CREATE TABLE transactions (
    Transaction_ID serial not null primary key,
    Customer_Card_ID bigint,
    Transaction_Summ numeric,
    Transaction_DateTime timestamp,
    Transaction_Store_ID bigint,
    constraint fk_transactions_customer_card_id foreign key (Customer_Card_ID) references cards(Customer_Card_ID)
);

CREATE TABLE SKU_group (
    Group_ID serial not null primary key,
    Group_Name varchar
);

CREATE TABLE product_grid (
    SKU_ID serial not null primary key,
    SKU_Name varchar,
    Group_ID bigint,
    constraint fk_product_grid_group_id foreign key (Group_ID) references SKU_group(Group_ID)
);

CREATE TABLE checks (
    Transaction_ID bigint,
    SKU_ID bigint not null,
    SKU_Amount numeric,
    SKU_Summ numeric,
    SKU_Summ_Paid numeric,
    SKU_Discount numeric,
    constraint fk_checks_transaction_id foreign key (Transaction_ID) references transactions(Transaction_ID)
--    constraint fk_checks_sku_id foreign key (SKU_ID) references product_grid(SKU_ID)
);


CREATE TABLE stores (
    Transaction_Store_ID serial,
    SKU_ID bigint,
    SKU_Purchase_Price numeric,
    SKU_Retail_Price numeric,
    constraint fk_stores_sku_id foreign key (SKU_ID) references product_grid(SKU_ID)
);

CREATE TABLE analysis_formation_date (
    Analysis_Formation timestamp
);

ALTER TABLE checks ADD CONSTRAINT fk_checks_sku_id FOREIGN KEY (sku_id)
REFERENCES product_grid(sku_id);

CREATE OR REPLACE PROCEDURE ImportData(
    IN table_name varchar,
    IN file_path varchar,
    IN delimiter varchar,
    IN header boolean
)
AS $$
    BEGIN
        EXECUTE 'SET datestyle TO "ISO, DMY"';
        IF header THEN
            EXECUTE format('COPY %I FROM %L WITH CSV HEADER DELIMITER %L', table_name, file_path, delimiter);
        ELSE
            EXECUTE format('COPY %I FROM %L DELIMITER %L', table_name, file_path, delimiter);
        END IF;
        EXECUTE 'RESET datestyle';
    END;
$$ LANGUAGE plpgsql;
                                               -- put your own file path here
CALL ImportData('personal_info', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/dataset/Personal_Data_Mini.tsv', E'\t', false);
CALL ImportData('cards', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/dataset/Cards_Mini.tsv', E'\t', false);
CALL ImportData('transactions', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/dataset/Transactions_Mini.tsv', E'\t', false);
CALL ImportData('checks', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/dataset/Checks_Mini.tsv', E'\t', false);
CALL ImportData('sku_group', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/dataset/groups_SKU_Mini.tsv', E'\t', false);
CALL ImportData('product_grid', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/dataset/SKU_Mini.tsv', E'\t', false);
CALL ImportData('stores', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/dataset/Stores_Mini.tsv', E'\t', false);
CALL ImportData('analysis_formation_date', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/dataset/Date_Of_Analysis_Formation.tsv', E'\t', false);


CREATE OR REPLACE PROCEDURE ExportData(
    IN table_name varchar,
    IN file_path varchar,
    IN delimiter varchar,
    IN header boolean
)
AS $$
    BEGIN
        IF header THEN
            EXECUTE format('COPY %I TO %L WITH CSV HEADER DELIMITER %L', table_name, file_path, delimiter);
        ELSE
            EXECUTE format('COPY %I TO %L DELIMITER %L', table_name, file_path, delimiter);
        END IF;
    END;
$$ LANGUAGE plpgsql;
                                               -- put your own file path here
CALL ExportData('personal_info', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/myset_sql/personal_info.tsv', E'\t', false);
CALL ExportData('cards', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/myset_sql/cards.csv', ';', true);
CALL ExportData('transactions', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/myset_sql/transactions.tsv', E'\t', false);
CALL ExportData('checks', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/myset_sql/checks.tsv', E'\t', false);
CALL ExportData('sku_group', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/myset_sql/sku_group.tsv', E'\t', false);
CALL ExportData('product_grid', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/myset_sql/product_grid.tsv', E'\t', false);
CALL ExportData('stores', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/myset_sql/stores.tsv', E'\t', false);
CALL ExportData('analysis_formation_date', '/Users/' || current_user || '/SQL3_RetailAnalitycs_v1.0-1/src/myset_sql/analysis_formation_date.tsv', E'\t', false);
