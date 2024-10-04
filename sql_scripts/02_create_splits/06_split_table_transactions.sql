-- Train
CREATE TABLE customer_analysis_se.combined_transactions_train AS
SELECT *
FROM customer_analysis_se.vw_daily_customer_aggregates_with_labels
WHERE label_date <= '2022-06-01' AND day <= '2022-06-01';

-- Test
CREATE TABLE customer_analysis_se.combined_transactions_test AS
SELECT *
FROM customer_analysis_se.vw_daily_customer_aggregates_with_labels
WHERE label_date > '2022-06-01'  AND day > '2022-06-01';
