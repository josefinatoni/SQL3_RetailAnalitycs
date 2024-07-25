CREATE VIEW customers AS
    WITH avg_check AS (
            SELECT customer_id,
                   AVG(transaction_summ) AS Customer_Average_Check,
                   PERCENT_RANK() OVER (ORDER BY AVG(transaction_summ) DESC) AS rank
            FROM cards
            JOIN transactions USING (customer_card_id)
            GROUP BY customer_id
    ),
        avg_check_seg AS (
               SELECT customer_id,
                      Customer_Average_Check,
                      CASE
                          WHEN rank <= 0.1 THEN 'High'
                          WHEN rank <= 0.35 THEN 'Medium'
                          ELSE 'Low'
                      END AS Customer_Average_Check_Segment
               FROM avg_check
    ),
        vs_fr AS (
            SELECT customer_id,
                   EXTRACT(EPOCH FROM (MAX(transaction_datetime) - MIN(transaction_datetime)) /
                                      COUNT(transaction_id) / 86400)::numeric AS Customer_Frequency

            FROM transactions
             JOIN cards c ON c.customer_card_id = transactions.customer_card_id
             GROUP BY customer_id
        ),
        visit_frequency AS (
             SELECT customer_id,
                    Customer_Frequency,
                    CASE
                        WHEN PERCENT_RANK() OVER (ORDER BY Customer_Frequency) <= 0.1
                            THEN 'Often'
                        WHEN PERCENT_RANK() OVER (ORDER BY Customer_Frequency) <= 0.35
                            THEN 'Occasionally'
                        ELSE 'Rarely'
                    END AS Customer_Frequency_Segment
             FROM vs_fr
    ),
         churn_probability AS (
             SELECT c.customer_id,
                    EXTRACT(EPOCH FROM Analysis_Formation - MAX(transaction_datetime)) / 86400 AS Customer_Inactive_Period,
                    EXTRACT(EPOCH FROM Analysis_Formation - MAX(transaction_datetime)) / Customer_Frequency / 86400 AS Customer_Churn_Rate,
                    CASE
                        WHEN (EXTRACT(DAYS FROM Analysis_Formation - MAX(transaction_datetime)) / Customer_Frequency) BETWEEN 0 AND 2
                            THEN 'Low'
                        WHEN (EXTRACT(DAYS FROM Analysis_Formation - MAX(transaction_datetime)) / Customer_Frequency) BETWEEN 2 AND 5
                            THEN 'Medium'
                        ELSE 'High'
                    END AS Customer_Churn_Segment
             FROM analysis_formation_date
             JOIN transactions ON analysis_formation_date.analysis_formation > transactions.transaction_datetime
             JOIN cards c ON c.customer_card_id = transactions.customer_card_id
             JOIN visit_frequency ON visit_frequency.customer_id = c.customer_id
             GROUP BY c.customer_id, Analysis_Formation, visit_frequency.Customer_Frequency
    ),
         segment AS (
             SELECT DISTINCT pi.customer_id,
             ((CASE
                WHEN Customer_Average_Check_Segment = 'Low' THEN 0
                WHEN Customer_Average_Check_Segment = 'Medium' THEN 9
                WHEN Customer_Average_Check_Segment = 'High' THEN 18
            END) +
            (CASE
                 WHEN Customer_Frequency_Segment = 'Rarely' THEN 0
                 WHEN Customer_Frequency_Segment = 'Occasionally' THEN 3
                 WHEN Customer_Frequency_Segment = 'Often' THEN 6
            END) +
            (CASE
                 WHEN Customer_Churn_Segment = 'Low' THEN 1
                 WHEN Customer_Churn_Segment = 'Medium' THEN 2
                 WHEN Customer_Churn_Segment = 'High' THEN 3
            END)) AS Customer_Segment
             FROM cards
             INNER JOIN avg_check_seg ac ON ac.customer_id = cards.customer_id
             INNER JOIN visit_frequency vf ON vf.customer_id = cards.customer_id
             INNER JOIN churn_probability cp ON cards.customer_id = cp.customer_id
             INNER JOIN personal_info pi on cards.customer_id = pi.customer_id
             ORDER BY customer_id
         ),
        client_store AS (
            SELECT pi.customer_id AS customer_id,
                   c.customer_card_id AS customer_card_id,
                   t.transaction_store_id
            FROM transactions t
            JOIN cards c USING (customer_card_id)
            JOIN personal_info pi USING (customer_id)
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
        ),
        store_partitition AS (
            SELECT customer_id, transaction_store_id, st.store_transactions / allt.all_transactions AS partitition
            FROM
                (SELECT customer_id, cs.transaction_store_id, COUNT(transaction_id)::NUMERIC AS store_transactions
                FROM transactions t
                JOIN client_store cs ON cs.transaction_store_id = t.transaction_store_id AND cs.customer_card_id = t.customer_card_id
                GROUP BY customer_id, cs.transaction_store_id) AS st
            JOIN
                (SELECT customer_id, COUNT(transaction_id)::NUMERIC AS all_transactions
                FROM transactions t
                JOIN client_store cs ON cs.transaction_store_id = t.transaction_store_id AND cs.customer_card_id = t.customer_card_id
                GROUP BY customer_id) AS allt USING (customer_id)
            WHERE st.customer_id = allt.customer_id -- maybe can be omitted
        ),
        previous_three AS (
            SELECT customer_id, transaction_store_id, transaction_datetime
            FROM (SELECT customer_id,
                         transaction_store_id,
                         transaction_datetime,
                         ROW_NUMBER() OVER (PARTITION BY customer_id
                             ORDER BY transaction_datetime DESC) AS row_num
                  FROM transactions
                  JOIN cards c USING (customer_card_id)
                  JOIN personal_info pi USING (customer_id)) AS visits
            WHERE row_num <= 3
        ),
       primary_store AS (
            SELECT customer_id,
                   CASE
                        WHEN COUNT(DISTINCT pt.transaction_store_id) = 1
                            THEN MIN(pt.transaction_store_id)
                        ELSE (SELECT transaction_store_id FROM previous_three
                                        WHERE previous_three.customer_id = pt.customer_id GROUP BY transaction_store_id
                                        ORDER BY SUM(partitition), MAX(transaction_datetime) DESC LIMIT 1)
                   END AS Customer_Primary_Store
            FROM previous_three pt
            JOIN store_partitition USING (customer_id, transaction_store_id)
            GROUP BY pt.customer_id
        )

    SELECT customer_id,
           Customer_Average_Check, -- smth wrong with accuracy after ~7th sign after comma
           Customer_Average_Check_Segment,
           Customer_Frequency,
           Customer_Frequency_Segment,
           Customer_Inactive_Period,
           Customer_Churn_Rate, -- smth wrong with accuracy after ~7th sign after comma
           Customer_Churn_Segment,
           Customer_Segment,
           Customer_Primary_Store
    FROM personal_info
        LEFT JOIN avg_check_seg USING (customer_id)
        LEFT JOIN visit_frequency USING (customer_id)
        LEFT JOIN churn_probability USING (customer_id)
        LEFT JOIN segment USING (customer_id)
        LEFT JOIN primary_store USING(customer_id)
    ORDER BY 1;

select * from customers;
-- select transaction_id, customer_card_id, transaction_datetime from transactions where customer_card_id in (3, 22) order by 3;
-- select customer_id, t.* from transactions t join cards using (customer_card_id) order by 1;

