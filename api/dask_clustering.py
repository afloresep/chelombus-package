import dask.dataframe as dd
import dask.array as da
import numpy as np
from api.utils.helper_functions import ProgressTracker
import logging

logger = logging.getLogger(__name__)

class Cluster():
    def __init__(self, input_path: str, steps: list): 
        # Assert all steps are natural integers
        for i in steps:
            assert i > 0, f"Step cannot be a negative number: {i} "
            assert type(i) == "int", f"Step must be int, instead got: {type(i)}" 

        self.input_path = input_path
        # Steps or number of buckets per dimension 
        self.first_dim_steps = steps[0]
        self.second_dim_steps = steps[1]
        self.third_dim_steps = steps[2]

        # Percentiles to split the data into buckets
        self.first_dim_percenntiles = np.arange(0,102, (100/self.first_dim_steps))
        self.second_dim_percentiles = np.arange(0,102, (100/self.second_dim_steps))
        self.third_dim_percentiles = np.arange(0,102, (100/self.third_dim_steps))

        # TODO: Add support for other file types e.g csv 
        self.dataframe = dd.read_parquet(self.input_path)
        
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

    def assign_clusters_by_column(self, df, col, quantiles, cluster_col='cluster_id') -> list:
        """Compute percentiles for a column and assign cluster IDs to the entire DataFrame."""
        # Compute global percentiles
        percentiles = self.compute_percentiles(df, col, quantiles)
        
        # Assign clusters to all rows based on these percentiles
        df = df.map_partitions(self.assign_cluster_ids, col=col, percentiles=percentiles, cluster_col=cluster_col)
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


input_folder = "/mnt/samsung_2tb/mixed_data/output/pca_transformed_results/output/*.parquet"
print("Loading data")

# Read the data
df = dd.read_parquet(input_folder, engine='pyarrow')

print("Assign cluster by column for PCA_1")
# 1) First level clustering by PCA_1:
df_test, pca1_percentiles = assign_clusters_by_column(df, 'PCA_1', cluster_col='cluster_id')

print("Clustering for PCA 2")
with ProgressTracker(description="Clustering", total_files=50, interval=4) as tracker:
    for i in range(50):

        # 2) Second level clustering by PCA_2 within a bucket of PCA_1 for all buckets:
        pca2_percentiles = compute_percentiles(df_test[df_test['cluster_id'] == i], 'PCA_2', quantiles=np.arange(0,102,2))

        # Assign cluster_id_2 only to rows where cluster_id == i 
        df_test = assign_clusters_conditionally(df_test, filter_col='cluster_id', filter_val=i, col='PCA_2', cluster_col='cluster_id_2', percentiles=pca2_percentiles)

        # Update progress bar
        tracker.update_progress()

print("Clustering for PCA_3")
with ProgressTracker(description="Clustering", total_files=50, interval=4) as tracker:
    for i in range(50):

        # 2) Third level clustering by PCA_3 within a specific bucket of PCA_2
        # Compute percentiles for PCA_2, but only for the subset where cluster_id == i
        pca2_percentiles = compute_percentiles(df_test[df_test['cluster_id'] == i], 'PCA_3', quantiles=np.arange(0,102,2))

        # Assign cluster_id_2 only to rows where cluster_id == 1
        df_test = assign_clusters_conditionally(df_test, filter_col='cluster_id', filter_val=i, col='PCA_3', cluster_col='cluster_id_3', percentiles=pca2_percentiles)

        # Update progress bar
        tracker.update_progress()

df['combined_cluster_id'] = (df_test['cluster_id'] * 50**2) + (df_test['cluster_id_2'] * 50) + df_test['cluster_id_3']

df['combined_cluster_id'] = df['combined_cluster_id'].astype(int)

print("Cluster completed, saving results")
# After completing all clustering steps, repartition to 400 partitions
df = df.repartition(npartitions=400)

# Save as Parquet files
output_path = "/mnt/10tb_hdd/buckets_pca_dask"
df.to_parquet(output_path, engine='pyarrow', write_index=False)
