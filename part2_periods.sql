CREATE VIEW periods AS
    SELECT customer_id,
           group_id,
           MIN(transaction_datetime) AS first_group_purchase_date,
           MAX(transaction_datetime) AS last_group_purchase_date,
           COUNT(DISTINCT transaction_id) AS Group_Purchase,
           (EXTRACT (EPOCH FROM MAX(transaction_datetime) - MIN(transaction_datetime))::NUMERIC / 86400 + 1) /
            COUNT(DISTINCT transaction_id) AS Group_Frequency,
           COALESCE(MIN(CASE WHEN sku_discount = 0 THEN NULL ELSE sku_discount / sku_summ END), 0) AS Group_Min_Discount
    FROM checks
    JOIN transactions USING (transaction_id)
    JOIN product_grid USING (sku_id)
    JOIN cards USING (customer_card_id)
    GROUP BY 1, 2;

select * from periods;
