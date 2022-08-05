import logging

# Create a custom logger.
logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

# Create a stream handler.
c_handler = logging.StreamHandler()
c_handler.setLevel(logging.INFO)

# Set the format
c_format = logging.Formatter('%(levelname)s - %(message)s')
c_handler.setFormatter(c_format)

# Add the handler to the logger.
logger.addHandler(c_handler)


def set_log_file_handler(log_path):
    global logger

    logger_handlers = logger.handlers

    # If a file handler already exist, remove it.
    if len(logger_handlers) > 1:
        f_handler = logger_handlers[1]
        f_handler.close()
        logger.removeHandler(f_handler)

    # Create a file handler.
    f_handler = logging.FileHandler(log_path)
    f_handler.setLevel(logging.INFO)

    # Set the format.
    f_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_format)

    # Add the handler to the logger.
    logger.addHandler(f_handler)
