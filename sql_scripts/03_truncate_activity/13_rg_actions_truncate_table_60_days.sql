CREATE TABLE customer_analysis_se.combined_rg_actions_train_60d AS
SELECT *
FROM customer_analysis_se.vw_rg_actions_with_labels
WHERE (timestamp < label_date - INTERVAL '60 days' OR timestamp > label_date)
AND label_date <= '2022-06-01';
