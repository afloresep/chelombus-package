import os

def find_input_type(file_path):
      if file_path.endswith('csv'):
            return 'csv'
      elif file_path.endswith('txt'):
            return 'txt'
      elif file_path.endswith('cxsmiles'):
            return 'cxsmiles'
      else: 
            raise ValueError('Unsupported input file. Only .csv, .txt. and .cxsmiles files are supported')

def process_input(input_path):
    if os.path.isdir(input_path):
        # Process all files in the directory
        for file in os.listdir(input_path):
            file_path = os.path.join(input_path, file)
            if os.path.isfile(file_path):  # Optional: filter by file extensions
                yield file_path
    elif os.path.isfile(input_path):
        # Process a single file
        yield input_path
    else:
        raise ValueError(f"Invalid input path: {input_path}")

