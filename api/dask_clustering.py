import dask.dataframe as dd
import dask.array as da
import numpy as np
from api.utils.helper_functions import ProgressTracker
import logging

logger = logging.getLogger(__name__)

class PCAClusterAssigner():
    def __init__(self, input_path: str, steps: list): 
        # Assert all steps are natural integers
        for i in steps:
            assert isinstance(i, int), "Step in STEP_LISTS is not of type int"
            assert i > 0, f"Step cannot be a negative number: {i} "

        self.input_path = input_path
        # TODO: Add support for other file types e.g csv 
        self.dataframe = dd.read_parquet(self.input_path, engine="pyarrow")

    def load_data(self):
        """Load the parquet files into a Dask DataFrame""" 
        self.df = dd.read_parquet(self.input_path, engine="pyarrow")

    def compute_percentiles(self, df, col, quantiles):
        """Compute given percentiles for a column in a Dask DataFrame."""
        arr = df[col].to_dask_array(lengths=True)
        return da.percentile(arr, q=quantiles, method='linear', internal_method='tdigest').compute()

    def assign_cluster_ids(self, part, col, percentiles, cluster_col='cluster_id'):
        """Assign cluster IDs to a partition based on given percentiles."""
        cluster_ids = np.digitize(part[col], percentiles) - 1
        # Ensure no cluster index is out-of-range
        cluster_ids = np.where(cluster_ids == 50, 49, cluster_ids)
        part[cluster_col] = cluster_ids
        return part

    def assign_clusters_by_column(self, col, quantiles, cluster_col='cluster_id') -> list:
        """
        Compute percentiles for a column and assign cluster IDs to the entire DataFrame.
        Parameters
        ----------
        col : str
            Column name on which to base cluster assignments.
        cluster_col : str, optional
            Name of the new column that will store cluster IDs.
        quantiles : array_like, optional
            Sequence of quantiles to compute. Defaults to np.arange(0, 102, 2).
        
        Returns
        -------
        percentiles : np.ndarray
            Computed percentiles for the column.
        """
        # Compute global percentiles
        percentiles = self.compute_percentiles(self.dataframe, col, quantiles)
    
        # Assign clusters to all rows based on these percentiles
        df = self.dataframe.map_partitions(self.assign_cluster_ids, col=col, percentiles=percentiles, cluster_col=cluster_col)
        return df, percentiles

    def assign_clusters_conditionally(self, df, filter_col, filter_val, col, cluster_col, percentiles):
        """
        Assign cluster IDs for rows meeting a certain condition without creating a separate copy.
        This allows adding a second-level cluster ID (e.g. cluster_id_2) only for a subset of rows.
        """
        def _assign_conditionally(part, col, percentiles, filter_col, filter_val, cluster_col):
            mask = (part[filter_col] == filter_val)
            cluster_ids = np.digitize(part.loc[mask, col], percentiles) - 1
            cluster_ids = np.where(cluster_ids == 50, 49, cluster_ids)
            part.loc[mask, cluster_col] = cluster_ids
            return part

        # Assign clusters only to rows where filter_col == filter_val
        df = df.map_partitions(_assign_conditionally, col, percentiles, filter_col, filter_val, cluster_col)
        return df

    def save_data(self, output_folder,  partition_on=None):
        """
        Save the updated Dataframe to parquet
        
        Parameters
        ----------
        partition_on : list or str, optional
            Column(s) on which to partition the data when saving. 
        """
        self.df.to_parquet(output_folder, partition_on=partition_on)