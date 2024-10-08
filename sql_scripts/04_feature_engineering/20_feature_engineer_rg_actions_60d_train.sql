-- SQL commands go here
CREATE TABLE customer_analysis_se.combined_transactions_train_60d AS
SELECT *
FROM customer_analysis_se.vw_daily_customer_aggregates_with_labels
WHERE (day < label_date - INTERVAL '60 days' OR day > label_date)
AND label_date <= '2022-06-01';
