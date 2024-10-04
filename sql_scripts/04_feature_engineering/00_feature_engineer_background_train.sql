-- Add 'age' and 'customer_length_weeks' columns to the table

ALTER TABLE customer_analysis_se.swedish_customer_background
ADD COLUMN age INT,
ADD COLUMN customer_length_weeks INT;

-- Calculate and set the 'age' of customers based on their birth dates

UPDATE customer_analysis_se.swedish_customer_background
SET age = EXTRACT(YEAR FROM AGE(current_date, birth_date));

-- Compute and store the duration of customer relationship in weeks since their first deposit

UPDATE customer_analysis_se.swedish_customer_background
SET customer_length_weeks = FLOOR(EXTRACT(DAY FROM AGE(current_date, first_deposit_timestamp)) / 7);
