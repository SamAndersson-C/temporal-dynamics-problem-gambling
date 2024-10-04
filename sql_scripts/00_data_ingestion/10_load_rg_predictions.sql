CREATE TABLE customer_analysis_se.combined_rg_prediction AS
SELECT crp.*
FROM combined_rg_prediction crp
JOIN customer_analysis_se.swedish_customers sc ON crp.customer_id = sc.customer_id;
