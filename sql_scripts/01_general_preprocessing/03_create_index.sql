-- For combined_sessions
CREATE INDEX idx_combined_sessions_customer_id ON customer_analysis_se.combined_sessions (customer_id);

-- For combined_bets
CREATE INDEX idx_combined_bets_customer_id ON customer_analysis_se.combined_bets (customer_id);

-- For combined_payments
CREATE INDEX idx_combined_payments_customer_id ON customer_analysis_se.combined_payments (customer_id);

-- For daily_customer_aggregates
CREATE INDEX idx_daily_customer_aggregates_customer_id ON customer_analysis_se.daily_customer_aggregates (customer_id);

-- For loss_chasing_analysis_results
CREATE INDEX idx_loss_chasing_analysis_results_customer_id ON customer_analysis_se.loss_chasing_analysis_results (customer_id);

-- For manual_assessments
CREATE INDEX idx_manual_assessments_customer_id ON customer_analysis_se.manual_assessments (customer_id);

-- For multiple_accounts
CREATE INDEX idx_multiple_accounts_customer_id ON customer_analysis_se.multiple_accounts (customer_id);

-- For rg_actions
CREATE INDEX idx_rg_actions_customer_id ON customer_analysis_se.rg_actions (customer_id);

-- For swedish_customer_background
CREATE INDEX idx_swedish_customer_background_customer_id ON customer_analysis_se.swedish_customer_background (customer_id);
