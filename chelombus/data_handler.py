import gc 
import logging
import os 
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.decomposition import IncrementalPCA
from utils.helper_functions import find_input_type
from memory_profiler import profile
import numpy as np
import time
import tqdm
from fingerprint_calculator import FingerprintCalculator

def get_total_chunks(file_path, chunksize):
    """ Calculate number of chunks based on self.chunksize for tqdm 
    Maybe avoid for files that are too large >150 GB? Takes about ~2 minutes for such size
    You can also just add manually the amount of lines to get an estimation
    """
    print('Preparing tqdm...')
    # total_lines = sum(1 for _ in open(file_path)) - 1  # Subtract 1 for header
    total_lines = int(674075400) # Lines for the Enamine_REAL file ~664M compounds
    total_chunks = (int(total_lines) + int(chunksize) - 1) // int(chunksize)
    print('tqdm preparation done...')
    return total_chunks
    
class DataHandler:
    def __init__(self, file_path, chunksize):
        self.file_path = file_path
        self.chunksize = chunksize
        self.datatype = find_input_type(file_path)

    def get_total_lines(self):
        """Calculate the total number of lines in the file."""
        with open(self.file_path, 'r', encoding='utf-8') as f:
            return sum(1 for _ in f)

    def load_data(self):
        total_chunks = get_total_chunks(self.file_path, self.chunksize)

        # Dynamically dispatch the right method based on datatype
        if self.datatype == 'csv':
            return self._load_csv_data(), total_chunks
        
        elif self.datatype == 'cxsmiles' :
            return self._load_cxsmiles_data(), total_chunks
        else:
            raise ValueError(f"Unsupported file type: {self.datatype}")

    def _load_csv_data(self): # Generator object
        try:
            return pd.read_csv(self.file_path, chunksize=self.chunksize)

        except Exception as e:
            raise ValueError(f"Error reading file: {e}")
        
    def _load_txt_data(self):
        #TODO return data from txt file
        #it should work with load cxsmiles function
        try:
            pass
        except Exception as e:
            raise ValueError(f"Error reading file: {e}")
            

    def _load_cxsmiles_data(self):
        smiles_col_index = 1  # Index for the 'smiles' column (0-based)
        try:
            for chunk in pd.read_csv(
                self.file_path,
                sep='\t',
                header=None,         # No header row
                usecols=[smiles_col_index],
                chunksize=self.chunksize,
                dtype=str,
                engine='c',
                encoding='utf-8'
            ):
                # smiles_list = chunk[smiles_col].tolist() -> for when data has column names
                smiles_list = chunk.iloc[:, 0].tolist()  # Access the first column of the chunk
                yield smiles_list
        except Exception as e:
            raise ValueError(f"Error loading data: {e}")


    def extract_smiles_and_features(self, data):
            """ 
            Method for extracting smiles and features in pandas. `data` object needs to be pd.DataFrame. 
            Not optimal for very large datasets? So far I've tried with 10M samples and performed well
            input: 
            param: data. pd.Dataframe, .txt or .cxsmiles file 
            output:
            smiles = np.array(batch_size,)
            features = np.array(batch_size, features) 
            """
            return data
            # try: 
            #     data.columns = data.columns.str.lower() # for csv file
            #     try: 
            #         smiles_column = data.filter(like='smiles').columns[0]  # Gets the first matching column name
            #     except: 
            #         raise ValueError(f"No SMILES column found in {self.file_path}. Ensure there's a column named 'smiles'.")
                
            #     smiles_list = data[smiles_column]
            #     features_list = data.drop(columns=[smiles_column])
            
            # except: 
            #     # for .txt or cxsmiles files
            #     """
            #      This part assumes that the 'smiles' columns in the txt or cxsmile files is in first position
            #     """
            #     smiles_list = np.array(data)[:,0] # Return list of all first elements in the list of lists (data) -> list of smiles
            #     features_list = np.array(data)[:,1:]

            #     # return smiles_list, features_list
 
            
            # return np.array(smiles_list), np.array(features_list)
    
    def one_hot_encode(self, features): 
        """ 
        #TODO Idea is to one hot encode categorical features e.g. 'target value'
        and use it in PCA 
        """
        one_hot_encoder = ColumnTransformer(
            transformers=[
                ('cat', OneHotEncoder(), features)
            ], 
            remainder= 'passthrough' # Keep the rest of columns as is
        )

        oh_features = one_hot_encoder.fit_transform(features)
        return oh_features
    
    # @profile
    def process_chunk(self, idx, chunk, output_dir):
        """
        Process a single chunk of data by calculating fingerprints and saving them to a parquet file
        """
        try:
            # Check if chunk already exists
            fp_chunk_path = os.path.join(output_dir, f'batch_parquet/fingerprints_chunk_{idx}.parquet')
            if os.path.exists(fp_chunk_path):
                logging.info(f'Chunk {idx} already processed, skipping.')
                return            

            # Extract smiles and features from chunk
            smiles_list = self.extract_smiles_and_features(chunk)

            fp_calculator = FingerprintCalculator(smiles_list, 'mqn')

            # Calculate fingerprints
            fingerprints = fp_calculator.calculate_fingerprints()
    
            # Ensure output directories exist
            os.makedirs(os.path.join(output_dir, 'batch_parquet'), exist_ok=True)
            os.makedirs(os.path.join(output_dir, 'output'), exist_ok=True)

    #       #TODO: Add option to use .h5 files? .h5 files are too slow though...
    #       # Save fingerprints in HDF5 format
    #       with h5py.File(fp_chunk_path, 'w') as h5f:
    #           h5f.create_dataset('fingerprints', data=fingerprints)

    #        # Save smiles and features in HDF5 format
    #       with h5py.File(os.path.join(output_dir, f'features_chunks/smiles_features_chunk_{idx}.h5'), 'w') as h5f:
    #           h5f.create_dataset('smiles_list', data=np.array(smiles_list, dtype='S'))  # Store strings as bytes
    #           # h5f.create_dataset('features', data= np.array(features, dtype='S')) 

            # Create dataframe with smiles list
            smiles_dataframe= pd.DataFrame({
                'smiles': smiles_list, 
            })
            del smiles_list

            # Create dataframe with fingerprints values 
            fingerprint_df = pd.DataFrame(fingerprints.tolist(), columns = [f'fp_{i+1}' for i in range(42)])
            del fingerprints

            # Concat both df. 
            chunk_dataframe = pd.concat([smiles_dataframe, fingerprint_df], axis=1)
            del smiles_dataframe, fingerprint_df 

            # Save to parquet dataframe
            chunk_dataframe.to_parquet(fp_chunk_path, index=False)

            del chunk_dataframe
            gc.collect() # Collect garbage. Not sure if it makes a difference but just in case

        except Exception as e:
            logging.error(f"Error processing chunk {idx}: {e}", exc_info=True)

