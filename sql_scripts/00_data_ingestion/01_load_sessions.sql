-- SQL commands go here
CREATE TABLE customer_analysis_se.combined_sessions AS
SELECT cs.*
FROM combined_sessions cs
JOIN customer_analysis_se.swedish_customers sc ON cs.customer_id = sc.customer_id;
