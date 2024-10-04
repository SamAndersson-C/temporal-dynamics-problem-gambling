ALTER TABLE customer_analysis_se.combined_bets ADD COLUMN date_column DATE;
ALTER TABLE customer_analysis_se.combined_payments ADD COLUMN date_column DATE;
ALTER TABLE customer_analysis_se.combined_sessions ADD COLUMN date_column DATE;
ALTER TABLE customer_analysis_se.daily_customer_aggregates ADD COLUMN date_column DATE;
ALTER TABLE customer_analysis_se.loss_chasing_analysis_results ADD COLUMN date_column DATE;
ALTER TABLE customer_analysis_se.manual_assessments ADD COLUMN date_column DATE; -- If needed
ALTER TABLE customer_analysis_se.multiple_accounts ADD COLUMN date_column DATE;
ALTER TABLE customer_analysis_se.rg_actions ADD COLUMN date_column DATE;
ALTER TABLE customer_analysis_se.swedish_customer_background ADD COLUMN date_column DATE;


UPDATE customer_analysis_se.combined_bets cb
SET date_column = (
    SELECT ma.date
    FROM customer_analysis_se.manual_assessments ma
    WHERE ma.customer_id = cb.customer_id
      AND ma.date <= DATE(cb.timestamp) -- Extracting date part for comparison
    ORDER BY ma.date DESC
    LIMIT 1
);

-- Update combined_payments
UPDATE customer_analysis_se.combined_payments cp
SET date_column = (
    SELECT ma.date
    FROM customer_analysis_se.manual_assessments ma
    WHERE ma.customer_id = cp.customer_id
      AND ma.date <= DATE(cp.timestamp)
    ORDER BY ma.date DESC
    LIMIT 1
);

-- Update combined_sessions
UPDATE customer_analysis_se.combined_sessions cs
SET date_column = (
    SELECT ma.date
    FROM customer_analysis_se.manual_assessments ma
    WHERE ma.customer_id = cs.customer_id
      AND ma.date <= DATE(cs.day)
    ORDER BY ma.date DESC
    LIMIT 1
);

-- Update daily_customer_aggregates
UPDATE customer_analysis_se.daily_customer_aggregates dca
SET date_column = (
    SELECT ma.date
    FROM customer_analysis_se.manual_assessments ma
    WHERE ma.customer_id = dca.customer_id
      AND ma.date <= DATE(dca.timestamp)
    ORDER BY ma.date DESC
    LIMIT 1
);

-- Update loss_chasing_analysis_results
UPDATE customer_analysis_se.loss_chasing_analysis_results lcar
SET date_column = (
    SELECT ma.date
    FROM customer_analysis_se.manual_assessments ma
    WHERE ma.customer_id = lcar.customer_id
      AND ma.date <= DATE(lcar.period_end)
    ORDER BY ma.date DESC
    LIMIT 1
);

-- Update rg_actions
UPDATE customer_analysis_se.rg_actions rga
SET date_column = (
    SELECT ma.date
    FROM customer_analysis_se.manual_assessments ma
    WHERE ma.customer_id = rga.customer_id
      AND ma.date <= DATE(rga.timestamp)
    ORDER BY ma.date DESC
    LIMIT 1
);

