import pandas as pd
import numpy as np
import pickle
import os
from config import OUTPUT_FILE_PATH, CHUNKSIZE
from tdigest import TDigest

def get_percentiles(digestion_methods: list, steps_list: list) -> dict: 
    """
    Get percentiles for every PCA Component or dimension.
    :param digestion_methods: list with all the TDigest methods -should be 3 or 4- for each PCA Component or dimension
    :param step_size: List with number of steps to divide the percentiles for each PCA Component. 
    e.g. if step size list is [3.125, 10] then it will calculate:
    x_data.percentile(3.125) 
    x_data.percentile(6.5) 
    ...
    x_data.percentile(96.875)
    ----
    y_data.percentile(10) 
    y_data.percentile(20)
    ...
    y_data.percentile(90)
    """

    percentiles_list = {} 

    for i in range(len(digestion_methods)):
                percentiles_list[f'PCA_{i+1}'] = [digestion_methods[i].percentile(step) for step in np.arange(0, 100, (100/steps_list[i]))]

    return percentiles_list 

class DimensionalityReducer():

    @property
    def steps(self):
        """Getter for steps."""
        if self._steps is None:
            raise ValueError("Steps have not been set yet")
        return self._steps
    

    @steps.setter
    def steps(self, steps_list):
        """Setter for steps with validation."""
        if any(step <= 0 for step in steps_list):
            raise ValueError("All steps must be positive values.")
        self._steps = steps_list


    def _round_to_step(self,coordinate:float, min_value:float, max_value:float, step_size:float):
           
            # Map the coordinate to its closest value in the steps
            
            if coordinate < min_value: 
                    return min_value
            elif coordinate > max_value:
                    return max_value
            else: # min_value + Number of steps x Step Size
                    return (min_value) + (step_size)*round((coordinate - min_value)/step_size)
                  

    def digest_generator(self, dimensions:int): 
            """
            Function to generate TDigest() methods recursively depending on the number of PCA Components selected
            """
            if dimensions == 3 or dimensions == 4: 
               
                digest_methods = [None]*dimensions

                for dim in range(len(digest_methods)):
                    digest_methods[dim] = TDigest()
                
                return digest_methods
            
            else:
                raise ValueError("PCA_N_COMPONENTS can only be 3 or 4")

    def fit_coord_multidimensional(self, output:str , percentiles: dict):
            """
            Generalize fit_coordinates to handle more than 3 dimensions.
            This function will handle any number of dimensions.

            :param output: The CSV file containing the PCA coordinates.
            :param percentiles: A dict with list of percentile ranges for each dimension.
            """
            
            df_output = pd.read_csv(os.path.join(OUTPUT_FILE_PATH, ('output/'+output)))

            for dim in range(len(percentiles)):
                step = (percentiles[dim][1] - percentiles[dim][0]) / self.steps[dim]
                pca_column = f'PCA_{dim + 1}'
                # Apply rounding for each dimension
                df_output[pca_column] = df_output[pca_column].apply(
                lambda x: self._round_to_step(x, percentiles[dim][0], percentiles[dim][1], step)
                )
            
            output_path = os.path.join(OUTPUT_FILE_PATH, ('output/'+output))

            df_output.to_csv(output_path, index=False)
            
    # Helper function to find the closest index
    def find_closest_index(my_point, sorted_list):
        """
        Binary tree search to find the closest number in the list of percentiles -which is sorted-. 
        This way we 'fit' to the grid. 
        """
        low, high = 0, len(sorted_list) - 1
        
        while low <= high:
            mid = (low + high) // 2
            if sorted_list[mid] == my_point:
                return mid
            elif sorted_list[mid] < my_point:
                low = mid + 1
            else:
                high = mid - 1

        # Determine the closest index
        if low >= len(sorted_list):
            return len(sorted_list) - 1
        if high < 0:
            return 0

        return high if abs(sorted_list[high] - my_point) <= abs(sorted_list[low] - my_point) else low
    
