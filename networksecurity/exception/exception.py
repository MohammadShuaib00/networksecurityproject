import sys
import logging

# Assuming you have a logging configuration defined elsewhere
# from networksecurity.logger.logging import logging

def error_message_details(error, error_detail):
   
    exc_type, exc_value, exc_tb = error_detail
    file_name = exc_tb.tb_frame.f_code.co_filename
    line_number = exc_tb.tb_lineno

    return (
        f"Error occurred in script '{file_name}', "
        f"line number {line_number}, "
        f"error message: {str(error)}"
    )


class NetworkSecurityException(Exception):
    
    def __init__(self, error_message, error_detail=sys.exc_info()):
        super().__init__(error_message)
        self.error_message = error_message_details(error_message, error_detail)

    def __str__(self):
        return self.error_message


# # Example usage
# if __name__ == "__main__":
#     try:
#         x = 1 / 0  # Intentional error to demonstrate the exception handling
#     except Exception as e:
#         logging.error("Exception occurred", exc_info=True)
#         raise CustomException("An error occurred while executing", sys.exc_info())
