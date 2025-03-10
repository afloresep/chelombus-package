import gc 
import logging
import os 
import pandas as pd
from api.utils.helper_functions import find_input_type
from api.fingerprint_calculator import FingerprintCalculator
from rdkit import Chem
from rdkit.Chem import Descriptors, rdMolDescriptors

def compute_descriptors(smiles: str):
    """Compute RDKit descriptors for a single SMILES. Returns a dict or None."""
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return {
            'hac': None,
            'num_aromatic_atoms': None,
            'fraction_aromatic_atoms': None,
            'number_of_rings': None,
            'molecular_weight': None,
            'clogP': None,
            'fraction_Csp3': None
        }
    hac = mol.GetNumHeavyAtoms()
    num_aromatic_atoms = sum(atom.GetIsAromatic() for atom in mol.GetAtoms())
    fraction_aromatic_atoms = num_aromatic_atoms / hac if hac > 0 else 0
    number_of_rings = rdMolDescriptors.CalcNumRings(mol)
    molecular_weight = Descriptors.ExactMolWt(mol)
    clogP = Descriptors.MolLogP(mol)
    fraction_Csp3 = Descriptors.FractionCSP3(mol)

    return {
        'hac': hac,
        'num_aromatic_atoms': num_aromatic_atoms,
        'fraction_aromatic_atoms': fraction_aromatic_atoms,
        'number_of_rings': number_of_rings,
        'molecular_weight': molecular_weight,
        'clogP': clogP,
        'fraction_Csp3': fraction_Csp3
    }

class DataHandler:
    def __init__(self, 
                 file_path: str, 
                 chunksize: int, 
                 fingerprint: str = 'mqn',  
                 smiles_col_index: int =0, 
                 header: int =0):
        self.file_path = file_path        
        self.fingerprint = fingerprint
        self.chunksize = chunksize
        self.smiles_col_index= smiles_col_index
        self.header = header
        self.datatype = find_input_type(file_path)

    @staticmethod 
    def get_total_chunks(file_path, chunksize):
        """ 
        Calculate number of chunks based on self.chunksize for tqdm 
        Maybe avoid for files that are too large >150 GB? Takes about ~2 minutes for such size
        You can also just add manually the amount of lines to get an estimation
        """
        # total_lines = sum(1 for _ in open(file_path)) - 1  # Subtract 1 for header
        total_lines = 955_000_000
        total_chunks = (int(total_lines) + int(chunksize) - 1) // int(chunksize)
        return total_chunks

    @staticmethod
    def get_total_lines(self):
        """Calculate the total number of lines in the file."""
        with open(self.file_path, 'r', encoding='utf-8') as f:
            return sum(1 for _ in f)
    
    def load_data(self):
        """Returns correct generator to load data based on file input type"""
        total_chunks = self.get_total_chunks(self.file_path, self.chunksize)
        # Dynamically dispatch the right method based on datatype
        if self.datatype == 'csv':
            return self._load_csv_data(), total_chunks
        
        elif self.datatype == 'cxsmiles' or self.datatype =='txt' :
            return self._load_cxsmiles_data(), total_chunks
        else:
            raise ValueError(f"Unsupported file type: {self.datatype}")

    def _load_csv_data(self): # Generator object
        try:
            return pd.read_csv(self.file_path, chunksize=self.chunksize)

        except Exception as e:
            raise ValueError(f"Error reading file: {e}")
        
    def _load_txt_data(self):
        #TODO return data from txt file. Will probably work with load cxsmiles function
        try:
            for chunk in pd.read_csv(
                self.file_path,
                sep='\t',
                header=self.header,
                usecols=[self.smiles_col_index],
                chunksize=self.chunksize,
                dtype=str,
                engine='c',
                encoding='utf-8'
            ):
                # smiles_list = chunk[smiles_col].tolist() -> for when data has column names
                smiles_list = chunk.iloc[:, 0].tolist()  # access the first column of the chunk
                yield smiles_list
        except Exception as e:
            raise ValueError(f"error loading data: {e}")

    def _load_cxsmiles_data(self):
        try:
            for chunk in pd.read_csv(
                self.file_path,
                sep='\t',
                header=self.header,
                usecols=[self.smiles_col_index],
                chunksize=self.chunksize,
                dtype=str,
                engine='c',
                encoding='utf-8'
            ):
                # smiles_list = chunk[smiles_col].tolist() -> for when data has column names
                smiles_list = chunk.iloc[:, 0].tolist()  # access the first column of the chunk
                yield smiles_list
        except Exception as e:
            raise ValueError(f"error loading data: {e}")

    def process_chunk(self, idx, chunk, output_dir, descriptors=False):
        """
        Process a single chunk of data by calculating fingerprints and saving them to a parquet file
        """

        # The name of the input file should be added as prefix to the batch output path so there's no 
        # collision in the naming with other files used as input as well

        filename = self.file_path.split('/')[-1].split('.')[0] # From '/path/path2/my_filename.txt' To 'my_filename'

        # Create a folder to save the fingerprints 

        try:
            fp_chunk_path = os.path.join(output_dir, f'{filename}_fp_chunk_{idx}.parquet')
            if os.path.exists(fp_chunk_path):
                logging.info(f'Chunk {idx} already processed, skipping.')
                return
            
            ##########################
            # Calculate fingerprints
            ##########################
            # chunk has a smiles column? 
            try:
                smiles_list = chunk['smiles']
            except:
                smiles_list = chunk

            fp_calculator = FingerprintCalculator(smiles_list, self.fingerprint)

            # Calculate fingerprints
            fingerprints = fp_calculator.calculate_fingerprints()

            if descriptors:
                ######################
                # Calculate descriptors
                ###################### 
                # a) Parallel apply on the 'smiles' column to get a list of dicts
                desc_series = smiles_list.parallel_apply(compute_descriptors)
                # b) Convert that Series of dicts into a DataFrame
                descriptors_df = pd.DataFrame(desc_series.tolist())

            ######################################################################
            # Prepare final DataFrame with SMILES, fingerprints, and descriptors 
            ######################################################################

            # Create a DataFrame for SMILES
            smiles_dataframe = pd.DataFrame({'smiles': smiles_list})
            
            # Create DataFrame for fingerprints 
            fingerprint_df = pd.DataFrame(
                fingerprints.tolist(), 
                columns=[f'fp_{i+1}' for i in range(len(fingerprints[1]))]  # or however many columns you have
            )

            if descriptors:
                # Concatenate all of them: SMILES + fingerprints + descriptors
                chunk_dataframe = pd.concat([smiles_dataframe, fingerprint_df, descriptors_df], axis=1)
                del descriptors_df
            else:
                # Concatenate SMILES + fingerprints
                chunk_dataframe = pd.concat([smiles_dataframe, fingerprint_df ], axis=1)

            # Save to parquet dataframe
            chunk_dataframe.to_parquet(fp_chunk_path, index=False)

            del chunk_dataframe, smiles_dataframe, smiles_list
            del fingerprint_df  
            gc.collect() # Collect garbage. Not sure if it makes a difference but just in case

        except Exception as e:
            logging.error(f"Error processing chunk {idx}: {e}", exc_info=True)

