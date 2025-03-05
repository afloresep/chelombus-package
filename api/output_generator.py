import numpy as np
import pandas as pd
import os
from typing import List, Tuple, Any, Optional
import logging
logger = logging.getLogger(__name__)

class OutputGenerator:
    def __init__(self):
        self._steps: Optional[List[float]] = None

    @property
    def steps(self) -> List[float]:
        """Getter for steps."""
        if self._steps is None:
            raise ValueError("Steps have not been set yet")
        return self._steps

    @steps.setter
    def steps(self, steps_list: List[float]) -> None:
        """Setter for steps with validation."""
        if any(step <= 0 for step in steps_list):
            raise ValueError("All steps must be positive values.")
        self._steps = steps_list

    def save_batch_csv(
        self,
        batch_idx: int,
        coordinates: np.ndarray,
        smiles_list: List[str],
        output_dir: str
    ) -> None:
        """
        Save the batch output as a CSV file.

        This will generate a CSV file for every batch.
        Not recommended for very large files as it will increase I/O.

        :param batch_idx: Index of the current batch.
        :param coordinates: NumPy array of coordinates.
        :param smiles_list: List of SMILES strings.
        :param features: Additional features (type unspecified).
        :param output_dir: Directory to save the output files.
        """
        batch_data = pd.DataFrame({'smiles': smiles_list})

        # Append PCA coordinates
        for i in range(coordinates.shape[1]):
            batch_data[f'PCA_{i+1}'] = coordinates[:, i]

        # TODO: Add features to DataFrame using 'features' parameter.

        output_path = os.path.join(output_dir, 'output', f'batch_data_{batch_idx}.csv')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        batch_data.to_csv(output_path, index=False)


    def batch_to_multiple_parquet(
        self,
        file_name, 
        coordinates: np.ndarray,
        smiles_list: List[str],
        output_dir: str
    ) -> None:
        """
        Save each batch into a single Parquet file for every chunk loaded.

        :param coordinates: NumPy array of coordinates.
        :param smiles_list: List of SMILES strings.
        :param features: Additional features (type unspecified).
        :param output_dir: Directory to save the output files.
        """
        parquet_path = os.path.join(output_dir , f'pca_vectors_{file_name}.parquet')

        # Create DataFrame for the current batch
        batch_data = pd.DataFrame({'smiles': smiles_list})
        del smiles_list

        # Append PCA coordinates
        for i in range(coordinates.shape[1]):
            batch_data[f'PCA_{i+1}'] = coordinates[:, i]
        del coordinates
        
        # TODO: Add features to DataFrame using 'features' parameter if needed.
        if os.path.exists(parquet_path):
            logger.info(f"{parquet_path} exists, skipping...")
        else:
            batch_data.to_parquet(parquet_path, engine="pyarrow")

        del batch_data 

    def batch_to_one_parquet(
        self,
        coordinates: np.ndarray,
        smiles_list: List[str],
        output_dir: str
    ) -> None:
        """
        Save each batch into a single Parquet file by merging them every time a chunk is loaded.

        :param coordinates: NumPy array of coordinates.
        :param smiles_list: List of SMILES strings.
        :param features: Additional features (type unspecified).
        :param output_dir: Directory to save the output files.
        """
        os.makedirs(os.path.join(output_dir, 'output'), exist_ok=True)
        parquet_path = os.path.join(output_dir, 'output', 'output_dataframe.parquet')

        # Create DataFrame for the current batch
        batch_data = pd.DataFrame({'smiles': smiles_list})

        # Append PCA coordinates
        for i in range(coordinates.shape[1]):
            batch_data[f'PCA_{i+1}'] = coordinates[:, i]

        # TODO: Add features to DataFrame using 'features' parameter if needed.

        if os.path.exists(parquet_path):
            # Load the existing Parquet file
            existing_parquet = pd.read_parquet(parquet_path)
            # Append the new batch to the existing Parquet file
            concatenated_parquet = pd.concat([existing_parquet, batch_data], ignore_index=True)
            del existing_parquet
        else:
            # If Parquet file doesn't exist, start with the new batch
            concatenated_parquet = batch_data

        concatenated_parquet.to_parquet(parquet_path, engine="pyarrow")
        del concatenated_parquet, batch_data