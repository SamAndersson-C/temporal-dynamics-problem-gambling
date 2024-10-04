-- Train
CREATE TABLE customer_analysis_se.combined_rg_prediction_train AS
SELECT *
FROM customer_analysis_se.vw_rg_prediction_with_labels
WHERE label_date <= '2022-06-01' AND date <= '2022-06-01';

-- Test
CREATE TABLE customer_analysis_se.combined_rg_prediction_test AS
SELECT *
FROM customer_analysis_se.vw_rg_prediction_with_labels
WHERE label_date > '2022-06-01' AND date > '2022-06-01';