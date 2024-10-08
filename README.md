
---

# Gambling Project: Data Ingestion, Preprocessing, and Analysis Pipeline

## Overview

This project automates a full data pipeline for ingesting raw data, performing preprocessing, feature engineering, and running analysis and modeling workflows. The project consists of SQL scripts for database operations, Python scripts for executing data processing tasks, and Jupyter notebooks for model development and exploration.

## Project Structure

```bash
project_directory/
├── notebooks/                     # Jupyter notebooks for analysis and exploration
│   ├── final_tuning_notebook.ipynb
│   ├── final8_modelling_notebook.ipynb
│   ├── final_preprocessing_and_selection_notebook.ipynb
│   ├── final_statistical_analysis.ipynb
│   └── final_statistical_analysis_notebook.ipynb
│
├── scripts/                       # Python scripts for running the pipeline
│   ├── run_sql_scripts.py         # Executes SQL scripts for data preprocessing
│   ├── data_processing.py         # Handles data loading, merging, and feature selection
│   ├── run_hyperparameter_tuning.py # Runs the hyperparameter tuning notebook
│   └── run_all.py                 # Master script to run the full pipeline
│
├── sql_scripts/                   # SQL scripts for data ingestion and preprocessing
│   ├── 00_data_ingestion/         # Ingest raw data from various sources
│   │   ├── 01_load_sessions.sql
│   │   ├── 02_load_bets.sql
│   │   ├── 03_load_payments.sql
│   │   ├── 04_load_multiple_accounts.sql
│   │   ├── 05_load_manual_assessments.sql
│   │   ├── 06_load_rg_actions.sql
│   │   ├── 07_load_daily_aggregate_transactions.sql
│   │   ├── 08_load_loss_chasing.sql
│   │   ├── 09_load_customer_background.sql
│   │   └── 10_load_rg_predictions.sql
│   │
│   ├── 01_general_preprocessing/  # General cleaning and preprocessing tasks
│   │   ├── 00_add_label_dates.sql
│   │   ├── 01_background.sql
│   │   ├── 02_filter_customers_by_country.sql
│   │   └── 03_create_index.sql
│   │
│   ├── 02_create_splits/          # SQL scripts for train-test split creation
│   │   ├── 01_split_table_manual_assessments.sql
│   │   ├── 02_split_table_background.sql
│   │   ├── 03_split_table_bets.sql
│   │   ├── 04_split_table_payments.sql
│   │   ├── 05_split_table_sessions.sql
│   │   ├── 06_split_table_transactions.sql
│   │   ├── 07_split_table_loss_chasing.sql
│   │   ├── 08_split_table_rg_actions.sql
│   │   ├── 09_split_table_rg_predictions.sql
│   │
│   ├── 03_truncate_activity/      # SQL scripts for truncating user activity
│   │   ├── 00_bets_truncate_table_30_days.sql
│   │   ├── 01_bets_truncate_table_60_days.sql
│   │   ├── 02_bets_truncate_table_90_days.sql
│   │   ├── 03_payments_truncate_table_30_days.sql
│   │   ├── 04_payments_truncate_table_60_days.sql
│   │   ├── 05_payments_truncate_table_90_days.sql
│   │   ├── 06_transactions_truncate_table_30_days.sql
│   │   ├── 07_transactions_truncate_table_60_days.sql
│   │   ├── 08_transactions_truncate_table_90_days.sql
│   │   ├── 09_loss_chasing_truncate_table_30_days.sql
│   │   ├── 10_loss_chasing_truncate_table_60_days.sql
│   │   ├── 11_loss_chasing_truncate_table_90_days.sql
│   │   ├── 12_rg_actions_truncate_table_30_days.sql
│   │   ├── 13_rg_actions_truncate_table_60_days.sql
│   │   ├── 14_rg_actions_truncate_table_90_days.sql
│   │   ├── 15_sessions_truncate_table_30_days.sql
│   │   ├── 16_sessions_truncate_table_60_days.sql
│   │   └── 17_sessions_truncate_table_90_days.sql
│   │
│   ├── 04_feature_engineering/    # SQL scripts for feature engineering
│   │   ├── 00_feature_engineer_background_train.sql
│   │   ├── 01_feature_engineer_background_test.sql
│   │   ├── 02_deposits_minus_withdrawals.sql
│   │   ├── 03_feature_engineer_bets_train.sql
│   │   ├── 04_feature_engineer_bets_30d_train.sql
│   │   ├── 05_feature_engineer_bets_60d_train.sql
│   │   ├── 06_feature_engineer_bets_90d_train.sql
│   │   ├── 07_feature_engineer_bets_test.sql
│   │   ├── 08_feature_engineer_payments_train.sql
│   │   ├── 09_feature_engineer_payments_30d_train.sql
│   │   ├── 10_feature_engineer_payments_60d_train.sql
│   │   ├── 11_feature_engineer_payments_90d_train.sql
│   │   ├── 12_feature_engineer_payments_test.sql
│   │   ├── 13_feature_engineer_transactions_train.sql
│   │   ├── 14_feature_engineer_transactions_30d_train.sql
│   │   ├── 15_feature_engineer_transactions_60d_train.sql
│   │   ├── 16_feature_engineer_transactions_90d_train.sql
│   │   ├── 17_feature_engineer_transactions_test.sql
│   │   ├── 18_feature_engineer_rg_actions_train.sql
│   │   ├── 19_feature_engineer_rg_actions_30d_test.sql
│   │   ├── 20_feature_engineer_rg_actions_60d_test.sql
│   │   ├── 21_feature_engineer_rg_actions_90d_test.sql
│   │   ├── 22_feature_engineer_rg_actions_test.sql
│
├── requirements.txt               # Python dependencies
├── README.md                      # Main project documentation
├── LICENSE                        # License (optional)
├── .gitignore                     # Files/folders to ignore in Git
```

## SQL Scripts in `sql_scripts/`

### `00_data_ingestion/`

This folder contains SQL scripts for loading raw data into the database from various sources.

- **01_load_sessions.sql**: Loads raw session data for users.
- **02_load_bets.sql**: Loads raw betting transactions.
- **03_load_payments.sql**: Loads raw payment transaction data.
- **04_load_multiple_accounts.sql**: Handles data related to multiple user accounts.
- **05_load_manual_assessments.sql**: Ingests manual assessment data.
- **06_load_rg_actions.sql**: Loads responsible gambling (RG) actions.
- **07_load_daily_aggregate_transactions.sql**: Loads daily aggregated transactional data.
- **08_load_loss_chasing.sql**: Ingests data on loss-chasing behaviors.
- **09_load_customer_background.sql**: Loads customer background data.
- **10_load_rg_predictions.sql**: Loads responsible gambling predictions data.

### `01_general_preprocessing/`

This folder contains SQL scripts for general preprocessing tasks.

- **00_add_label_dates.sql**: Adds label dates to

 specific events.
- **01_background.sql**: Prepares customer background data for further analysis.
- **02_filter_customers_by_country.sql**: Filters customer data by country.
- **03_create_index.sql**: Creates database indexes to improve query performance.

### `02_create_splits/`

This folder contains SQL scripts for creating train-test splits from various tables.

- **01_split_table_manual_assessments.sql**: Splits the manual assessments data into training and testing sets.
- **02_split_table_background.sql**: Splits the customer background data into training and testing sets.
- **03_split_table_bets.sql**: Splits betting transactions into train and test sets.
- **04_split_table_payments.sql**: Splits payment data into training and testing sets.
- **05_split_table_sessions.sql**: Splits session data into train and test sets.
- **06_split_table_transactions.sql**: Splits transaction data into training and testing sets.
- **07_split_table_loss_chasing.sql**: Splits data related to loss-chasing behavior into train and test sets.
- **08_split_table_rg_actions.sql**: Splits responsible gambling actions data into training and testing sets.
- **09_split_table_rg_predictions.sql**: Splits responsible gambling predictions data into training and testing sets.

### `03_truncate_activity/`

This folder contains SQL scripts for truncating user activity data to specific periods (30, 60, and 90 days).

- **00_bets_truncate_table_30_days.sql**: Truncates the bets table for the last 30 days.
- **01_bets_truncate_table_60_days.sql**: Truncates the bets table for the last 60 days.
- **02_bets_truncate_table_90_days.sql**: Truncates the bets table for the last 90 days.
- **03_payments_truncate_table_30_days.sql**: Truncates the payments table for the last 30 days.
- **04_payments_truncate_table_60_days.sql**: Truncates the payments table for the last 60 days.
- **05_payments_truncate_table_90_days.sql**: Truncates the payments table for the last 90 days.
- **06_transactions_truncate_table_30_days.sql**: Truncates the transactions table for the last 30 days.
- **07_transactions_truncate_table_60_days.sql**: Truncates the transactions table for the last 60 days.
- **08_transactions_truncate_table_90_days.sql**: Truncates the transactions table for the last 90 days.
- **09_loss_chasing_truncate_table_30_days.sql**: Truncates the loss-chasing data for the last 30 days.
- **10_loss_chasing_truncate_table_60_days.sql**: Truncates the loss-chasing data for the last 60 days.
- **11_loss_chasing_truncate_table_90_days.sql**: Truncates the loss-chasing data for the last 90 days.
- **12_rg_actions_truncate_table_30_days.sql**: Truncates responsible gambling actions data for the last 30 days.
- **13_rg_actions_truncate_table_60_days.sql**: Truncates responsible gambling actions data for the last 60 days.
- **14_rg_actions_truncate_table_90_days.sql**: Truncates responsible gambling actions data for the last 90 days.
- **15_sessions_truncate_table_30_days.sql**: Truncates session data for the last 30 days.
- **16_sessions_truncate_table_60_days.sql**: Truncates session data for the last 60 days.
- **17_sessions_truncate_table_90_days.sql**: Truncates session data for the last 90 days.

### `04_feature_engineering/`

This folder contains SQL scripts for feature engineering. These scripts create new features from raw data to be used in downstream machine learning models. The scripts handle both training and testing data, and they generate features based on various time windows (30, 60, 90 days).

- **00_feature_engineer_background_train.sql**: Feature engineering on background data for the training set.
- **01_feature_engineer_background_test.sql**: Feature engineering on background data for the test set.
- **02_deposits_minus_withdrawals.sql**: Calculates the net balance of deposits minus withdrawals.
- **03_feature_engineer_bets_train.sql**: General feature engineering on bets data for the training set.
- **04_feature_engineer_bets_30d_train.sql**: Feature engineering on bets data for the last 30 days (training set).
- **05_feature_engineer_bets_60d_train.sql**: Feature engineering on bets data for the last 60 days (training set).
- **06_feature_engineer_bets_90d_train.sql**: Feature engineering on bets data for the last 90 days (training set).
- **07_feature_engineer_bets_test.sql**: General feature engineering on bets data for the test set.
- **08_feature_engineer_payments_train.sql**: General feature engineering on payments data (training set).
- **09_feature_engineer_payments_30d_train.sql**: Feature engineering on payments for the last 30 days (training set).
- **10_feature_engineer_payments_60d_train.sql**: Feature engineering on payments for the last 60 days (training set).
- **11_feature_engineer_payments_90d_train.sql**: Feature engineering on payments for the last 90 days (training set).
- **12_feature_engineer_payments_test.sql**: General feature engineering on payments data (test set).
- **13_feature_engineer_transactions_train.sql**: General feature engineering on transactions data (training set).
- **14_feature_engineer_transactions_30d_train.sql**: Feature engineering on transactions for the last 30 days (training set).
- **15_feature_engineer_transactions_60d_train.sql**: Feature engineering on transactions for the last 60 days (training set).
- **16_feature_engineer_transactions_90d_train.sql**: Feature engineering on transactions for the last 90 days (training set).
- **17_feature_engineer_transactions_test.sql**: General feature engineering on transactions data (test set).
- **18_feature_engineer_rg_actions_train.sql**: General feature engineering on responsible gambling actions data (training set).
- **19_feature_engineer_rg_actions_30d_train.sql**: Feature engineering on responsible gambling actions data for the last 30 days (training set).
- **20_feature_engineer_rg_actions_60d_train.sql**: Feature engineering on responsible gambling actions data for the last 60 days (training set).
- **21_feature_engineer_rg_actions_90d_train.sql**: Feature engineering on responsible gambling actions data for the last 90 days (training set).
- **22_feature_engineer_rg_actions_test.sql**: General feature engineering on responsible gambling actions data (test set).


## Python Scripts in `scripts/`

### `run_sql_scripts.py`

This script sequentially executes the SQL scripts in the `sql_scripts/` folder. It logs the progress of each script and provides error handling for failures.

```bash
python scripts/run_sql_scripts.py
```

### `data_processing.py`

This script connects to the database, loads data from tables, merges datasets, and performs feature selection. The processed data is then exported as a CSV file.

### `run_hyperparameter_tuning.py`

Executes the hyperparameter tuning notebook by running `final_hyperparameter_tuning_notebook.ipynb` using Jupyter's batch mode. It logs the tuning process and results.

### `run_all.py`

This is the master script that orchestrates the entire pipeline, from SQL preprocessing to data processing and hyperparameter tuning. Run this script to execute the entire pipeline:

```bash
python scripts/run_all.py
```

## Jupyter Notebooks in `notebooks/`

This folder

 contains Jupyter notebooks that perform further data analysis and modeling. Each notebook corresponds to different stages of the analysis pipeline.

- **`final_tuning_notebook.ipynb`**: Fine-tunes the model parameters for optimal performance.
- **`final_modelling_notebook.ipynb`**: Trains and evaluates machine learning models.
- **`final_preprocessing_and_selection_notebook.ipynb`**: Handles data preprocessing and feature selection.
- **`final_statistical_analysis.ipynb`**: Performs statistical analysis on the dataset.
- **`final_statistical_analysis_notebook.ipynb`**: An additional notebook for statistical analysis.

## Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/YourUsername/ScientificProject.git
```

### 2. Install dependencies

Ensure you have all the required dependencies installed by running:

```bash
pip install -r requirements.txt
```

### 3. Database Setup

Before running the SQL scripts, configure your database connection in the `run_sql_scripts.py` and `data_processing.py` files by replacing `your_database_connection_string` with the actual connection string for your database.

### 4. Running the Full Pipeline

To run the entire pipeline (SQL ingestion, preprocessing, feature engineering, and model tuning), run the `run_all.py` script:

```bash
python scripts/run_all.py
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

