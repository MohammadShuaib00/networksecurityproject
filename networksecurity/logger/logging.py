import os
import sys
import logging
from datetime import datetime

# Generate a unique Log file name based on the current datetime
LOG_FILE = f"{datetime.now().strftime('%m_%d_%Y_H_%M_%S')}.log"

# Define the path to the log directory
log_path = os.path.join(os.getcwd(), "logs")

# Create the log directory if it doesn't exist
os.makedirs(log_path, exist_ok=True)

# Combine the log directory path with the log file name
LOG_FILE_PATH = os.path.join(log_path, LOG_FILE)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',  # Define log message format
    handlers=[
        logging.FileHandler(LOG_FILE_PATH, mode="a"),  # Log file (append mode)
        logging.StreamHandler(sys.stdout),  # Console output
    ],
)


