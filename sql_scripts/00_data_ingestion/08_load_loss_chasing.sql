CREATE TABLE customer_analysis_se.loss_chasing_analysis_results AS
SELECT lcar.*
FROM loss_chasing_analysis_results lcar
JOIN customer_analysis_se.swedish_customers sc ON lcar.customer_id = sc.customer_id;
