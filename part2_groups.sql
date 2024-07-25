CREATE OR REPLACE FUNCTION fnc_group_margin(p_period interval DEFAULT NULL, p_transaction_amount integer DEFAULT NULL)
    RETURNS TABLE (
        customer_id integer,
        group_id integer,
        Group_Margin numeric
    ) AS $$
    SELECT customer_id,
           group_id,
           SUM(group_summ_paid - group_cost) AS Group_Margin
    FROM (SELECT *, ROW_NUMBER() over (PARTITION BY customer_id, group_id ORDER BY transaction_datetime DESC) AS num_rows
          FROM purchase_history) AS numbered_ph
    WHERE (p_period IS NOT NULL AND p_transaction_amount IS NULL
               AND transaction_datetime >= (SELECT analysis_formation
                                            FROM analysis_formation_date))
    OR (p_period IS NULL AND p_transaction_amount IS NOT NULL AND p_transaction_amount <= num_rows)
    OR (p_period IS NOT NULL AND p_transaction_amount IS NOT NULL
            AND p_transaction_amount <= num_rows
            AND transaction_datetime >= (SELECT analysis_formation
                                         FROM analysis_formation_date))
    OR (p_period IS NULL AND p_transaction_amount IS NULL)
    GROUP BY 1, 2
$$ LANGUAGE sql;

CREATE VIEW groups AS
    WITH group_list AS (
        SELECT customer_id,
               pg.group_id
        FROM checks
        JOIN transactions USING (transaction_id)
        JOIN cards USING (customer_card_id)
        JOIN product_grid pg USING (sku_id)
        GROUP BY 1, 2
        ORDER BY 1, 2
        ),
        affinity AS (
            SELECT customer_id,
                   group_id,
                   group_purchase / (SELECT COUNT(transaction_id)
                                       FROM purchase_history ph
                                       WHERE group_list.customer_id = ph.customer_id AND
                                             (ph.transaction_datetime BETWEEN p.first_group_purchase_date AND p.last_group_purchase_date)
                                       GROUP BY customer_id)::numeric AS Group_Affinity_Index
            FROM group_list
            JOIN periods p USING (customer_id, group_id)
            ORDER BY 1, 2
        ),
        churn_rate AS (
            SELECT customer_id,
                   group_id,
                   EXTRACT(EPOCH FROM analysis_formation - MAX(transaction_datetime)) / 86400 / group_frequency AS Group_Churn_Rate
            FROM purchase_history ph
            JOIN periods USING (customer_id, group_id)
            JOIN analysis_formation_date afd ON afd.analysis_formation > ph.transaction_datetime
            GROUP BY 1, 2, analysis_formation, group_frequency
        ),
        intervals AS (
            SELECT customer_id,
                   group_id,
                   transaction_id,
                   EXTRACT(EPOCH FROM transaction_datetime - LAG(transaction_datetime)
                       OVER (PARTITION BY customer_id, group_id ORDER BY transaction_datetime)) / 86400 AS interval
            FROM purchase_history
        ),
        deviation AS (
            SELECT customer_id,
                   group_id,
                   CASE
                       WHEN interval - group_frequency > 0 THEN (interval - group_frequency) / group_frequency
                       WHEN interval - group_frequency < 0 THEN (interval - group_frequency) / group_frequency * -1
                   END AS absolute_deviation
            FROM intervals
            JOIN periods USING (customer_id, group_id)
        ),
        stability AS (
            SELECT customer_id,
                   group_id,
                   AVG(absolute_deviation) AS Group_Stability_Index
            FROM deviation
            GROUP BY 1, 2
        ),
        discount_transactions AS (
            SELECT customer_id,
                   group_id,
                   COUNT(DISTINCT transaction_id) AS tr_amnt
            FROM checks
            JOIN transactions USING (transaction_id)
            JOIN cards USING (customer_card_id)
            JOIN product_grid USING (sku_id)
            JOIN periods USING (customer_id, group_id)
            WHERE sku_discount > 0
            GROUP BY 1, 2
        ),
        transactions_share AS (
            SELECT customer_id,
                   group_id,
                   tr_amnt / periods.group_purchase::NUMERIC AS Group_Discount_Share
            FROM discount_transactions
            JOIN periods USING (customer_id, group_id)
        ),
        min_discount AS (
            SELECT customer_id,
                   group_id,
                   MIN(group_min_discount) AS Group_Minimum_Discount
            FROM periods
            WHERE group_min_discount <> 0
            GROUP BY 1, 2
        ),
        avg_discount AS (
            SELECT customer_id,
                   group_id,
                   SUM(group_summ_paid) / SUM(purchase_history.group_summ) AS Group_Average_Discount
            FROM purchase_history
            JOIN periods USING (customer_id, group_id)
            WHERE group_min_discount > 0 AND group_summ_paid <> group_summ
            GROUP BY 1, 2
            ORDER BY 1, 2
        )

SELECT customer_id,
       group_id,
       Group_Affinity_Index,
       Group_Churn_Rate,
       Group_Stability_Index,
       Group_Margin,
       Group_Discount_Share,
       Group_Minimum_Discount,
       Group_Average_Discount
FROM affinity
JOIN churn_rate USING (customer_id, group_id)
JOIN stability USING (customer_id, group_id)
JOIN transactions_share USING (customer_id, group_id)
JOIN min_discount USING (customer_id, group_id)
JOIN avg_discount USING (customer_id, group_id)
JOIN fnc_group_margin() USING (customer_id, group_id)
ORDER BY 1, 2;

select * from groups;

-- SELECT customer_id, count(transaction_id) FROM purchase_history
-- GROUP BY 1