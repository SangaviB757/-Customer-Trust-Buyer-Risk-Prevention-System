# -Customer-Trust-Buyer-Risk-Prevention-System
Detect and flag suspicious or potentially fraudulent e-commerce transactions using SQL-based analytical rules and behavioral heuristics in order to restore Customer Trust

### -Problem Statement

E-commerce platforms experience diverse fraudulent behaviors — multiple IP logins, mismatched devices, COD refund scams, and inconsistent chargebacks.
Traditional transaction data alone is insufficient for detecting such risks without cross-referencing login, refund, chargeback, and product-level information.

### - The challenge:

Build a SQL-based Risk Flagging System that identifies and labels transactions as “Risky” or “Non-Risky” by analyzing multiple behavioral factors.

### 11 Fraud Detection Problem Statements

#### 1) Rapid-Fire Transactions

Problem:
A customer places multiple transactions within 10–15 minutes from different IP addresses on the same day.
Such rapid activity from varying IPs indicates possible account sharing, bot usage, or hijacked sessions.

Goal: Detect back-to-back transactions within 15 minutes from different IPs for the same customer.

#### 2)  Multiple IP Addresses in a Single Day

Problem:
A customer uses multiple IP addresses to place orders on the same day, suggesting the use of VPNs or identity masking.

Goal: Identify same-day transactions with different IP addresses for the same user.

#### 3)  Login–Transaction Device/IP Mismatch

Problem:
The device ID or IP address used during login doesn’t match those used during the transaction.
This signals session hijacking or use of stolen credentials.

Goal: Detect inconsistencies between login records and transaction details on the same date.

#### 4) High-Value Product Purchase

Problem:
A transaction involves a product in the top 5% price range of its category — could indicate a high-risk purchase by a potentially fraudulent buyer.

Goal: Flag all purchases involving top 5% high-value items by price per product category.

#### 5)  Chargeback Amount Mismatch

Problem:
The chargeback amount differs significantly from the original transaction amount, or it deviates abnormally from historical averages.
This may reflect fake chargeback claims or billing disputes manipulation.

Goal: Identify transactions with chargeback amounts unusually high/low compared to transaction total or customer’s chargeback history.

#### 6) COD Order Refund Abuse

Problem:
A Cash-on-Delivery (COD) order is marked “Delivered”, but the buyer claims a refund stating “Item not received”.
Common sign of return/refund fraud or logistic manipulation.

Goal: Flag all COD transactions where delivery is confirmed but a refund claim is made for “Item not received.”

#### 7) Multiple Payment Methods in 24 Hours

Problem:
A buyer uses more than two different payment methods within a 24-hour window, showing signs of account testing, stolen card trials, or fraudulent behavior.

Goal: Identify transactions by customers who used more than two payment types within 24 hours.

#### 8) New Customer Making High-Value Orders

Problem:
A newly registered user (joined within 7 days) purchases products in the top 2% price range, indicating possible stolen account or bonus abuse.

Goal: Flag new customers (<7 days old) making top 2% high-value purchases.

#### 9) High-Quantity Orders

Problem:
A single transaction involves very large quantities of a product (≥ 7 items).
Such bulk orders might represent reselling, bot activity, or fraudulent stock clearing.

Goal: Flag transactions where quantity ≥ 7.

#### 10) Suspicious Refund After Prepaid Transaction

Problem:
Customer requests a refund within 3 days of a prepaid purchase (not COD), claiming late delivery — indicating possible refund exploitation.

Goal: Identify prepaid transactions where refund is requested within 3 days of purchase.

#### 11) Address or Country Mismatch

Problem:
The delivery address country doesn’t match the registered customer country — suggesting proxy shipping, fake addresses, or international smuggling attempts.

Goal: Detect transactions where delivery address country ≠ customer’s registered country.
