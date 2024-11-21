import numpy as np
import pandas as pd
import pickle
import os
from typing import List, Tuple, Any, Optional
from chelombus.utils.config import OUTPUT_FILE_PATH, CHUNKSIZE

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
        features: Any,
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
        idx, 
        coordinates: np.ndarray,
        smiles_list: List[str],
        features: Any,
        output_dir: str
    ) -> None:
        """
        Save each batch into a single Parquet file for every chunk loaded.

        :param coordinates: NumPy array of coordinates.
        :param smiles_list: List of SMILES strings.
        :param features: Additional features (type unspecified).
        :param output_dir: Directory to save the output files.
        """
        os.makedirs(os.path.join(output_dir, 'output'), exist_ok=True)
        parquet_path = os.path.join(output_dir, 'output', f'output_dataframe_{idx}.parquet')

        # Create DataFrame for the current batch
        batch_data = pd.DataFrame({'smiles': smiles_list})
        del smiles_list

        # Append PCA coordinates
        for i in range(coordinates.shape[1]):
            batch_data[f'PCA_{i+1}'] = coordinates[:, i]
        del coordinates
        
        # TODO: Add features to DataFrame using 'features' parameter if needed.
        if os.path.exists(parquet_path):
            print(f"{parquet_path} exists, skipping...")
        else:
            batch_data.to_parquet(parquet_path, engine="pyarrow")

        del batch_data 

    def batch_to_one_parquet(
        self,
        coordinates: np.ndarray,
        smiles_list: List[str],
        features: Any,
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

    def _round_to_step(
        self,
        coordinate: float,
        min_value: float,
        max_value: float,
        step_size: float
    ) -> float:
        """
        Map the coordinate to its closest value in the steps.

        :param coordinate: The coordinate value to round.
        :param min_value: Minimum allowed value.
        :param max_value: Maximum allowed value.
        :param step_size: The size of each step.
        :return: The rounded coordinate value.
        """
        if coordinate < min_value:
            return min_value
        elif coordinate > max_value:
            return max_value
        else:
            return min_value + step_size * round((coordinate - min_value) / step_size)

    def fit_coord_multidimensional(
        self,
        output: str,
        percentiles: List[Tuple[float, float]]
    ) -> None:
        """
        Generalize fit_coordinates to handle more than 3 dimensions.

        :param output: The CSV file containing the PCA coordinates.
        :param percentiles: A list of percentile ranges for each dimension.
        """
        if len(percentiles) != len(self.steps):
            raise ValueError("Percentiles and steps must have the same number of dimensions")

        df_output = pd.read_csv(os.path.join(OUTPUT_FILE_PATH, 'output', output))

        for dim in range(len(percentiles)):
            step = (percentiles[dim][1] - percentiles[dim][0]) / self.steps[dim]
            pca_column = f'PCA_{dim + 1}'
            # Apply rounding for each dimension
            df_output[pca_column] = df_output[pca_column].apply(
                lambda x: self._round_to_step(x, percentiles[dim][0], percentiles[dim][1], step)
            )

        output_path = os.path.join(OUTPUT_FILE_PATH, 'output', output)
        df_output.to_csv(output_path, index=False)
