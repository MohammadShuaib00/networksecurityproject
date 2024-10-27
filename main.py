from networksecurity.logger.logging import logging
from networksecurity.exception.exception import CustomException
import os,sys
# Example usage
if __name__ == "__main__":
    try:
        x = 1 / 0  # Intentional error to demonstrate the exception handling
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
        raise CustomException("An error occurred while executing", sys.exc_info())