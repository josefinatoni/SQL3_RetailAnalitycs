CREATE VIEW purchase_history AS
    SELECT customer_id,
           transaction_id,
           transaction_datetime,
           group_id,
           SUM(sku_amount * stores.sku_purchase_price) AS group_cost,
           SUM(sku_summ) AS group_summ,
           SUM(sku_summ_paid) AS group_summ_paid
    FROM personal_info
    JOIN cards USING (customer_id)
    JOIN transactions USING (customer_card_id)
    JOIN checks USING (transaction_id)
    JOIN product_grid USING (sku_id)
    JOIN stores USING (sku_id)
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3, 4;

SELECT * FROM purchase_history;