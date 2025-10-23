create view transaction_risk_flagged as

-- 1) Back-to-back transactions within 10–15 minutes from different IPs 
    
with H_Risky_behaviour as (
    with cte as (
        select *,
               lag(transaction_date) over(partition by customer_id, date(transaction_date) order by transaction_date) as previous_order,
               lag(ip_address) over(partition by customer_id, date(transaction_date) order by transaction_date) as previous_ip_address
        from transactions
    ),
    cte2 as (
        select *,
               datediff('minutes', previous_order, transaction_date) as minutes_diff
        from cte
    )
    select transaction_id
    from cte2
    where previous_order is not null
      and minutes_diff <= 15
      and ip_address != previous_ip_address
),

-- 2) Transactions on same day with multiple IP addresses  ---- ip_fraud

Multiple_ip as (
    select distinct transaction_id
    from (
        select transaction_id,
               transaction_date,
               ip_address,
               lag(transaction_date) over(partition by customer_id order by transaction_date) as previous_order,
               lag(ip_address) over(partition by customer_id order by transaction_date) as previous_ip_address,
               datediff('day', previous_order, transaction_date) as days_diff
        from transactions
    ) t
    where days_diff = 0
      and ip_address != previous_ip_address
),

-- 3) Device ID / IP mismatch between transaction and login on same date login_transaction_device_mismatch


login_transaction_Ipmismatch as (
    select t.transaction_id
    from transactions as t
    join login_activity as l 
      on t.customer_id = l.customer_id
     and l.login_timestamp >= date_trunc('day', t.transaction_date)
     and l.login_timestamp < date_trunc('day', t.transaction_date) + interval '1 day'
    where t.ip_address != l.ip_address
       or t.device_id  != l.device_id
       or l.ip_city   != t.billing_city
),

-- 4) Purchasing high-value products (top 5% per category)  High_value_products

high_value_purchase as (
    with High_value_Products as (
        select product_id
        from (
            select product_id,
                   category,
                   product_name,
                   price,
                   cume_dist() over(partition by category order by price desc) as rank
            from products
        ) t
        where rank <= 0.05
    )
    select transaction_id
    from transactions
    where product_id in (select product_id from High_value_Products)
),

-- 5) Chargeback amount inconsistent with transaction amount

chargeback_amt_mismatch as (
    with chargeback_claim as (
        select t.transaction_id,
               t.total_amount,
               c.chargeback_amount,
               round((total_amount * 0.1), 2) as threshold_price,
               avg(chargeback_amount) over(partition by c.customer_id order by chargeback_date rows between 5 preceding and 1 preceding)  as rolling_avg,
               max(chargeback_amount) over(partition by c.customer_id order by chargeback_date rows between 5 preceding and 1 preceding)  as rolling_max
        from transactions as t
        inner join chargeback as c on t.transaction_id = c.transaction_id
    )
    select transaction_id
    from chargeback_claim
    where chargeback_amount >= (total_amount + threshold_price)
       or chargeback_amount <= (total_amount - threshold_price)
       or chargeback_amount >= rolling_avg * 1.5
       or chargeback_amount >= rolling_max
),

-- 6) COD order marked delivered but refund claimed (Item not received)

not_delievered_refund as (
    select t.transaction_id
    from transactions as t
    inner join refund as r on t.transaction_id = r.transaction_id
    join customer as c on t.customer_id = c.customer_id
    where payment_method = 'Cash on Delivery'
      and delivery_status = 'Delivered'
      and refund_reason = 'Item not received'
      and t.billing_city = c.city
),

-- 7) Multiple payment methods in short span (> 2 methods in 24h)

muliple_methods_pay as (
    select t1.transaction_id
    from transactions t1
    join transactions t2
      on t1.customer_id = t2.customer_id
     and t2.transaction_date between dateadd('hour', -24, t1.transaction_date) and t1.transaction_date
    group by t1.transaction_id
    having count(distinct t2.payment_method) > 2
),

-- 8) New customer placing high-value orders (≤ 7 days from join, top 2% product)

new_cust_high_purchase as (
    with High_value_orders as (
        select product_id,
               cume_dist() over(order by price desc) as rnk
        from products
    )
    select t.transaction_id
    from transactions as t
    inner join customer as c on t.customer_id = c.customer_id
    where datediff('day', join_date, date(transaction_date)) <= 7
      and product_id in (
          select product_id from High_value_orders where rnk <= 0.02
      )
),

-- 9) High quantity orders

high_quantity_order as (
    select t.transaction_id
    from transactions as t
    join products as p on t.product_id = p.product_id
    where quantity >= 7
),

-- 10) Refund requested very soon after prepaid transaction (≤ 3 days, Late Delivery)

    late_delivery_refund as (
    with prepaid as (
        select distinct payment_method
        from transactions
        where payment_method not in ('Cash on Delivery', 'Gift Card')
    )
    select t.transaction_id
    from transactions as t
    inner join refund as r on t.transaction_id = r.transaction_id
    where payment_method in (select payment_method from prepaid)
      and datediff('day', date(transaction_date), refund_date) <= 3
),
 
 
 
Address_mismatch as
(
                select * from 
                        (
                        select 
                            t.transaction_id, 
                            t.customer_id, 
                            c.country, 
                            Trim(SPLIT_PART(delivery_address,',',4)) as country_transaction
                        from transactions as t
                        inner join customer as c on
                        t.customer_id = c.customer_id
                        )t
                        where country_transaction != country
                        )


-- Union of all risky transaction IDs

,RiskyTransactions as (
    select transaction_id from H_Risky_behaviour
    union
    select transaction_id from Multiple_ip
    union
    select transaction_id from login_transaction_Ipmismatch
    union
    select transaction_id from high_value_purchase
    union
    select transaction_id from chargeback_amt_mismatch
    union
    select transaction_id from not_delievered_refund
    union
    select transaction_id from muliple_methods_pay
    union
    select transaction_id from new_cust_high_purchase
    union
    select transaction_id from high_quantity_order
    union
    select transaction_id from late_delivery_refund
    union
     select transaction_id from Address_mismatch
)


-- Final result -- Flagging the Records-----

select t.*,
             
        case when rt.transaction_id is not null then 'Risky'
            else 'Non Risky' end as Possibility,
         case when rt.transaction_id in (select transaction_id from H_Risky_behaviour) then 1 else 0 end as flag_rapid_fire_txns,
         case when rt.transaction_id in (select transaction_id from Multiple_ip) then 1 else 0 end as flag_multi_ip_same_day,
         case when rt.transaction_id in (select transaction_id from login_transaction_Ipmismatch) then 1 else 0 end as flag_device_ip_LoginTrans_mismatch,
         case when rt.transaction_id in (select transaction_id from high_value_purchase ) then 1 else 0 end as flag_high_value_order,
         case when rt.transaction_id in (select transaction_id from chargeback_amt_mismatch) then 1 else 0 end as flag_chargeback_mismatch,
         case when rt.transaction_id in (select transaction_id from not_delievered_refund) then 1 else 0 end as flag_cod_not_delivered_claim,
         case when rt.transaction_id in (select transaction_id from muliple_methods_pay ) then 1 else 0 end as flag_multi_payment_methods,
         case when rt.transaction_id in (select transaction_id from  new_cust_high_purchase) then 1 else 0 end as flag_new_customer_high_value,
         case when rt.transaction_id in (select transaction_id from  high_quantity_order) then 1 else 0 end as flag_high_quantity_order,
         case when rt.transaction_id in (select transaction_id from late_delivery_refund) then 1 else 0 end as flag_suspicious_refund_request,
         case when rt.transaction_id in (select transaction_id from Address_mismatch) then 1 else 0 end as flag_home_delivery_mismatch
         
from transactions t
left join RiskyTransactions rt
on t.transaction_id = rt.transaction_id 
