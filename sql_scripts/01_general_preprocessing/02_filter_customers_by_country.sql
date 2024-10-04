-- Filter customers by Sweden
CREATE TABLE customer_analysis_se.swedish_customers AS
SELECT customer_id
FROM customer_background
WHERE country_code = 'SE';

ALTER TABLE customer_analysis_se.swedish_customers
RENAME TO swedish_customers_backup;

CREATE TABLE customer_analysis_se.temp_swedish_customers AS
SELECT DISTINCT customer_id
FROM customer_analysis_se.swedish_customers_backup;

ALTER TABLE customer_analysis_se.temp_swedish_customers
RENAME TO swedish_customers;
