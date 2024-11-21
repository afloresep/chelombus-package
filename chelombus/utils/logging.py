import logging

def setup_logging(log_level, log_file=None):
    """
    Configures logging to output to console and optionally to a file.
    """
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    handlers = [logging.StreamHandler()]  # Always log to console

    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(level=log_level, format=log_format, handlers=handlers)
