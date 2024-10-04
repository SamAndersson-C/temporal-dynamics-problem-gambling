CREATE TABLE customer_analysis_se.daily_customer_aggregates AS
SELECT dca.*
FROM daily_customer_aggregates dca
JOIN customer_analysis_se.swedish_customers sc ON dca.customer_id = sc.customer_id;
