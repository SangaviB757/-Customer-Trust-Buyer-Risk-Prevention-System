/*
============================================================
Project: Buyer Risk Prevention System
Author: Your Name
Description: SQL View to flag risky buyer transactions using 11 fraud detection rules.
============================================================
*/

CREATE VIEW transaction_risk_flagged AS

-- ============================================================
-- 1️) Rapid-Fire Transactions: Back-to-back within 10–15 minutes from different IPs
-- ============================================================
WITH H_Risky_behaviour AS (
    WITH cte AS (
        SELECT *,
               LAG(transaction_date) OVER (
                   PARTITION BY customer_id, DATE(transaction_date)
                   ORDER BY transaction_date
               ) AS previous_order,
               LAG(ip_address) OVER (
                   PARTITION BY customer_id, DATE(transaction_date)
                   ORDER BY transaction_date
               ) AS previous_ip_address
        FROM transactions
    ),
    cte2 AS (
        SELECT *,
               DATEDIFF('minutes', previous_order, transaction_date) AS minutes_diff
        FROM cte
    )
    SELECT transaction_id
    FROM cte2
    WHERE previous_order IS NOT NULL
      AND minutes_diff <= 15
      AND ip_address != previous_ip_address
),

-- ============================================================
-- 2) Multiple IPs on Same Day
-- ============================================================
Multiple_ip AS (
    SELECT DISTINCT transaction_id
    FROM (
        SELECT transaction_id,
               transaction_date,
               ip_address,
               LAG(transaction_date) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS previous_order,
               LAG(ip_address) OVER (PARTITION BY customer_id ORDER BY transaction_date) AS previous_ip_address,
               DATEDIFF('day', previous_order, transaction_date) AS days_diff
        FROM transactions
    ) t
    WHERE days_diff = 0
      AND ip_address != previous_ip_address
),

-- ============================================================
-- 3) Login vs Transaction Device/IP Mismatch
-- ============================================================
login_transaction_Ipmismatch AS (
    SELECT t.transaction_id
    FROM transactions AS t
    JOIN login_activity AS l 
      ON t.customer_id = l.customer_id
     AND l.login_timestamp >= DATE_TRUNC('day', t.transaction_date)
     AND l.login_timestamp < DATE_TRUNC('day', t.transaction_date) + INTERVAL '1 day'
    WHERE t.ip_address != l.ip_address
       OR t.device_id  != l.device_id
       OR l.ip_city    != t.billing_city
),

-- ============================================================
-- 4️) High-Value Purchases: Top 5% per Category
-- ============================================================
high_value_purchase AS (
    WITH High_value_Products AS (
        SELECT product_id
        FROM (
            SELECT product_id,
                   category,
                   product_name,
                   price,
                   CUME_DIST() OVER (PARTITION BY category ORDER BY price DESC) AS rank
            FROM products
        ) t
        WHERE rank <= 0.05
    )
    SELECT transaction_id
    FROM transactions
    WHERE product_id IN (SELECT product_id FROM High_value_Products)
),

-- ============================================================
-- 5️) Chargeback Amount Mismatch
-- ============================================================
chargeback_amt_mismatch AS (
    WITH chargeback_claim AS (
        SELECT t.transaction_id,
               t.total_amount,
               c.chargeback_amount,
               ROUND((total_amount * 0.1), 2) AS threshold_price,
               AVG(chargeback_amount) OVER (
                   PARTITION BY c.customer_id
                   ORDER BY chargeback_date
                   ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
               ) AS rolling_avg,
               MAX(chargeback_amount) OVER (
                   PARTITION BY c.customer_id
                   ORDER BY chargeback_date
                   ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
               ) AS rolling_max
        FROM transactions AS t
        INNER JOIN chargeback AS c ON t.transaction_id = c.transaction_id
    )
    SELECT transaction_id
    FROM chargeback_claim
    WHERE chargeback_amount >= (total_amount + threshold_price)
       OR chargeback_amount <= (total_amount - threshold_price)
       OR chargeback_amount >= rolling_avg * 1.5
       OR chargeback_amount >= rolling_max
),

-- ============================================================
-- 6️) COD Refund Claimed After Delivery
-- ============================================================
not_delivered_refund AS (
    SELECT t.transaction_id
    FROM transactions AS t
    INNER JOIN refund AS r ON t.transaction_id = r.transaction_id
    JOIN customer AS c ON t.customer_id = c.customer_id
    WHERE payment_method = 'Cash on Delivery'
      AND delivery_status = 'Delivered'
      AND refund_reason = 'Item not received'
      AND t.billing_city = c.city
),

-- ============================================================
-- 7️) Multiple Payment Methods in 24 Hours
-- ============================================================
multiple_methods_pay AS (
    SELECT t1.transaction_id
    FROM transactions t1
    JOIN transactions t2
      ON t1.customer_id = t2.customer_id
     AND t2.transaction_date BETWEEN DATEADD('hour', -24, t1.transaction_date) AND t1.transaction_date
    GROUP BY t1.transaction_id
    HAVING COUNT(DISTINCT t2.payment_method) > 2
),

-- ============================================================
-- 8️) New Customer High-Value Orders (≤ 7 days from join, top 2%)
-- ============================================================
new_cust_high_purchase AS (
    WITH High_value_orders AS (
        SELECT product_id,
               CUME_DIST() OVER (ORDER BY price DESC) AS rnk
        FROM products
    )
    SELECT t.transaction_id
    FROM transactions AS t
    INNER JOIN customer AS c ON t.customer_id = c.customer_id
    WHERE DATEDIFF('day', join_date, DATE(transaction_date)) <= 7
      AND product_id IN (SELECT product_id FROM High_value_orders WHERE rnk <= 0.02)
),

-- ============================================================
-- 9️) High Quantity Orders
-- ============================================================
high_quantity_order AS (
    SELECT t.transaction_id
    FROM transactions AS t
    JOIN products AS p ON t.product_id = p.product_id
    WHERE quantity >= 7
),

-- ============================================================
-- 10) Refund Requested Soon After Prepaid (≤ 3 days)
-- ============================================================
late_delivery_refund AS (
    WITH prepaid AS (
        SELECT DISTINCT payment_method
        FROM transactions
        WHERE payment_method NOT IN ('Cash on Delivery', 'Gift Card')
    )
    SELECT t.transaction_id
    FROM transactions AS t
    INNER JOIN refund AS r ON t.transaction_id = r.transaction_id
    WHERE payment_method IN (SELECT payment_method FROM prepaid)
      AND DATEDIFF('day', DATE(transaction_date), refund_date) <= 3
),

-- ============================================================
-- 11) Address or Country Mismatch
-- ============================================================
Address_mismatch AS (
    SELECT *
    FROM (
        SELECT 
            t.transaction_id, 
            t.customer_id, 
            c.country, 
            TRIM(SPLIT_PART(delivery_address, ',', 4)) AS country_transaction
        FROM transactions AS t
        INNER JOIN customer AS c ON t.customer_id = c.customer_id
    ) t
    WHERE country_transaction != country
),

-- ============================================================
-- Union of All Risky Transactions
-- ============================================================
RiskyTransactions AS (
    SELECT transaction_id FROM H_Risky_behaviour
    UNION SELECT transaction_id FROM Multiple_ip
    UNION SELECT transaction_id FROM login_transaction_Ipmismatch
    UNION SELECT transaction_id FROM high_value_purchase
    UNION SELECT transaction_id FROM chargeback_amt_mismatch
    UNION SELECT transaction_id FROM not_delivered_refund
    UNION SELECT transaction_id FROM multiple_methods_pay
    UNION SELECT transaction_id FROM new_cust_high_purchase
    UNION SELECT transaction_id FROM high_quantity_order
    UNION SELECT transaction_id FROM late_delivery_refund
    UNION SELECT transaction_id FROM Address_mismatch
)

-- ============================================================
-- Final Output: Flagging Each Transaction
-- ============================================================
SELECT 
    t.*,
    CASE WHEN rt.transaction_id IS NOT NULL THEN 'Risky'
         ELSE 'Non Risky' END AS Possibility,

    CASE WHEN rt.transaction_id IN (SELECT transaction_id FROM H_Risky_behaviour) THEN 1 ELSE 0 END AS flag_rapid_fire_txns,
    CASE WHEN rt.transaction_id IN (SELECT transaction_id FROM Multiple_ip) THEN 1 ELSE 0 END AS flag_multi_ip_same_day,
    CASE WHEN rt.transaction_id IN (SELECT transaction_id FROM login_transaction_Ipmismatch) THEN 1 ELSE 0 END AS flag_device_ip_LoginTrans_mismatch,
    CASE WHEN rt.transaction_id IN (SELECT transaction_id FROM high_value_purchase) THEN 1 ELSE 0 END AS flag_high_value_order,
    CASE WHEN rt.transaction_id IN (SELECT transaction_id FROM chargeback_amt_mismatch) THEN 1 ELSE 0 END AS flag_chargeback_mismatch,
    CASE WHEN rt.transaction_id IN (SELECT transaction_id FROM not_delivered_refund) THEN 1 ELSE 0 END AS flag_cod_not_delivered_claim,
    CASE WHEN rt.transaction_id IN (SELECT transaction_id FROM multiple_methods_pay) THEN 1 ELSE 0 END AS flag_multi_payment_methods,
    CASE WHEN rt.transaction_id IN (SELECT transaction_id FROM new_cust_high_purchase) THEN 1 ELSE 0 END AS flag_new_customer_high_value,
    CASE WHEN rt.transaction_id IN (SELECT transaction_id FROM high_quantity_order) THEN 1 ELSE 0 END AS flag_high_quantity_order,
    CASE WHEN rt.transaction_id IN (SELECT transaction_id FROM late_delivery_refund) THEN 1 ELSE 0 END AS flag_suspicious_refund_request,
    CASE WHEN rt.transaction_id IN (SELECT transaction_id FROM Address_mismatch) THEN 1 ELSE 0 END AS flag_home_delivery_mismatch

FROM transactions t
LEFT JOIN RiskyTransactions rt
ON t.transaction_id = rt.transaction_id;

