import subprocess
import logging

# Set up logging
logging.basicConfig(
    filename='run_notebooks.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

def run_notebook(notebook_path):
    """Runs a Jupyter notebook and logs the result."""
    logging.info(f"Running notebook {notebook_path}")
    try:
        subprocess.run(
            ["jupyter", "nbconvert", "--to", "notebook", "--execute", notebook_path, "--output", notebook_path],
            check=True
        )
        logging.info(f"Successfully executed notebook: {notebook_path}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing notebook {notebook_path}: {e}")
        raise

def main():
    logging.info("Starting notebook execution pipeline")

    # Define the order of notebooks to run
    notebooks = [
        'notebooks/final_preprocessing_and_selection_notebook.ipynb',
        'notebooks/final_tuning_notebook.ipynb',
        'notebooks/final_modelling_notebook.ipynb',
        'notebooks/final_statistical_analysis_notebook.ipynb'
    ]

    # Execute each notebook in the specified order
    for notebook in notebooks:
        try:
            run_notebook(notebook)
        except Exception as e:
            logging.error(f"Pipeline failed at {notebook}: {e}")
            raise

    logging.info("All notebooks executed successfully")

if __name__ == "__main__":
    main()
