import logging

def setup_logging(log_level, log_file=None):
    """
    Configures logging to output to console and optionally to a file.
    log_level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
    log_file (str, optional): Path to a log file. If None, logs will not be written to a file.
    """
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    handlers = [logging.StreamHandler()]  # Always log to console

    if log_file:
        try:
            handlers.append(logging.FileHandler(log_file))
        except Exception as e:
            logging.error(f"Failed to create log file handler {e}")


    logging.basicConfig(level=log_level, format=log_format, handlers=handlers)
