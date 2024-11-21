import logging
from config import LOGGING_LEVEL, LOGGING_FORMAT, LOG_FILE_PATH

def setup_logging():
    logging.basicConfig(
        level=getattr(logging, LOGGING_LEVEL),
        format=LOGGING_FORMAT,
        filename=LOG_FILE_PATH  # To log to a file
    )



def find_input_type(file_path):
      if file_path.endswith('csv'):
            return 'csv'
      elif file_path.endswith('txt'):
            return 'txt'
      elif file_path.endswith('cxsmiles'):
            return 'cxsmiles'
      else: 
            raise ValueError('Unsupported input file. Only .csv, .txt. and .cxsmiles files are supported')

def validate_data():
    pass

def i_o_operations():
    pass

