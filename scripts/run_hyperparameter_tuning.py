import subprocess
import logging

# Set up logging
logging.basicConfig(
    filename='hyperparameter_tuning.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

def run_notebook(notebook_path):
    logging.info(f"Running notebook {notebook_path}")
    try:
        # Execute the Jupyter notebook
        subprocess.run(
            ["jupyter", "nbconvert", "--to", "notebook", "--execute", notebook_path, "--output", notebook_path],
            check=True
        )
        logging.info(f"Successfully executed notebook: {notebook_path}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error executing notebook {notebook_path}: {e}")
        raise

def main():
    logging.info("Starting hyperparameter tuning")
    notebook_path = 'notebooks/final_tuning_notebook.ipynb'

    try:
        run_notebook(notebook_path)
        logging.info("Hyperparameter tuning completed successfully")
    except Exception as e:
        logging.error(f"Hyperparameter tuning failed: {e}")
        raise

if __name__ == "__main__":
    main()
