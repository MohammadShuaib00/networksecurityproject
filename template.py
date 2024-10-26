import os
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="[%(asctime)s]: %(message)s")

projectname = "networksecurity"

list_of_files = [
    ".github/workflows/.gitkeep",  # GitHub Actions Workflow
    "NetworkData",  # Data directory
    f"{projectname}/__init__.py",  # Project init
    f"{projectname}/components/__init__.py",  # Components init
    f"{projectname}/components/data_ingestion.py",  # Data Ingestion
    f"{projectname}/components/data_transformation.py",  # Data Transformation
    f"{projectname}/components/data_validation.py",  # Data Validation
    f"{projectname}/components/model_trainer.py",  # Model Trainer
    f"{projectname}/components/model_evaluation.py",  # Model Evaluation
    # Exception handling
    f"{projectname}/exception/__init__.py",  # Exception handling init
    f"{projectname}/exception/exception.py",  # Custom exceptions
    # Logging
    f"{projectname}/logger/__init__.py",  # Logger init
    f"{projectname}/logger/logging.py",  # Logging configuration
    # Cloud-related operations
    f"{projectname}/cloud/__init__.py",  # Cloud operations init
    f"{projectname}/cloud/s3_operations.py",  # AWS S3 operations
    f"{projectname}/cloud/gcp_operations.py",  # Google Cloud operations (optional)
    # Pipeline scripts
    f"{projectname}/pipeline/__init__.py",
    f"{projectname}/pipeline/training_pipeline.py",  # Training pipeline
    f"{projectname}/pipeline/prediction_pipeline.py",  # Prediction pipeline
    # Configuration
    f"{projectname}/config/config.yaml",  # Config file
    # Artifacts storage
    f"{projectname}/artifacts",  # Artifacts directory
    # Utility files
    f"{projectname}/utils/__init__.py",  # Utils init
    f"{projectname}/utils/common.py",  # Utility functions
    # Docker-related files
    "Dockerfile",  # Dockerfile for containerization
    # Environment Variables
    ".env",  # Environment variables
    # Setup files
    "setup.py",  # Packaging and distribution
    # Git ignore
    ".gitignore",  # Git ignore file
    # ReadMe
    "README.md",  # Project description
    # Notebooks
    "notebooks/EDA.ipynb",  # Exploratory Data Analysis
    "notebooks/model_training.ipynb",  # Model training
    "notebooks/prediction.ipynb",  # Prediction notebook
]

for filepath in list_of_files:
    filepath = Path(filepath)
    filedir, filename = os.path.split(filepath)

    # Create directory if it doesn't exist
    if filedir != "":
        os.makedirs(filedir, exist_ok=True)
        logging.info(f"Directory created: {filedir}")

    # Create the file if it doesn't exist
    if (not filepath.exists()) or filepath.stat().st_size == 0:
        with open(filepath, "w") as f:
            pass  # Create an empty file
        logging.info(f"File created: {filepath}")
    else:
        logging.info(f"File already exists: {filepath}")
