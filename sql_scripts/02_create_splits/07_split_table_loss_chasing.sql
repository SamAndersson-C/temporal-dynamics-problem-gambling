-- Train
CREATE TABLE customer_analysis_se.combined_loss_chasing_train AS
SELECT *
FROM customer_analysis_se.vw_loss_chasing_analysis_results_with_labels
WHERE label_date <= '2022-06-01' AND period_end <= '2022-06-01';

-- Test
CREATE TABLE customer_analysis_se.combined_loss_chasing_test AS
SELECT *
FROM customer_analysis_se.vw_loss_chasing_analysis_results_with_labels
WHERE label_date > '2022-06-01' AND period_end > '2022-06-01';