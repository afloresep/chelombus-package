import dask.dataframe as dd
import dask.array as da
import numpy as np
from utils.helper_functions import FileProgressTracker

def compute_percentiles(df, col, quantiles):
    """Compute given percentiles for a column in a Dask DataFrame."""
    arr = df[col].to_dask_array(lengths=True)
    return da.percentile(
        arr,
        q=quantiles,
        method='linear',
        internal_method='tdigest'
    ).compute()

def assign_cluster_ids(part, col, percentiles, cluster_col='cluster_id'):
    """Assign cluster IDs to a partition based on given percentiles."""
    cluster_ids = np.digitize(part[col], percentiles) - 1
    # Ensure no cluster index is out-of-range
    cluster_ids = np.where(cluster_ids == 50, 49, cluster_ids)
    part[cluster_col] = cluster_ids
    return part

def assign_clusters_by_column(df, col, cluster_col='cluster_id', quantiles=np.arange(0,102,2)):
    """Compute percentiles for a column and assign cluster IDs to the entire DataFrame."""
    # Compute global percentiles
    percentiles = compute_percentiles(df, col, quantiles)
    
    # Assign clusters to all rows based on these percentiles
    df = df.map_partitions(assign_cluster_ids, col=col, percentiles=percentiles, cluster_col=cluster_col)
    return df, percentiles

def assign_clusters_conditionally(df, filter_col, filter_val, col, cluster_col, percentiles):
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


# ------------------------------------------
# Example Usage
# ------------------------------------------

input_folder = "/mnt/samsung_2tb/mixed_data/output/pca_transformed_results/output/*.parquet"

print("Loading data")
# Read the data
df = dd.read_parquet(input_folder, engine='pyarrow')

print("Assign cluster by column for PCA_2")
# 1) First level clustering by PCA_1:
df_test, pca1_percentiles = assign_clusters_by_column(df, 'PCA_1', cluster_col='cluster_id')

with FileProgressTracker(description="Clustering", total_files=50, interval=60) as tracker:
    for i in range(50):
        # 2) Second level clustering by PCA_2 within a specific bucket of PCA_1 (for example, bucket 1):
        # Compute percentiles for PCA_2, but only for the subset where cluster_id == 1
        pca2_percentiles = compute_percentiles(df_test[df_test['cluster_id'] == i], 'PCA_2', quantiles=np.arange(0,102,2))

        # Assign cluster_id_2 only to rows where cluster_id == 1
        df_test = assign_clusters_conditionally(df_test, filter_col='cluster_id', filter_val=i, col='PCA_2', cluster_col='cluster_id_2', percentiles=pca2_percentiles)

        # At this point, `df` has both cluster_id (from PCA_1) and cluster_id_2 (from PCA_2 for bucket 1) columns.

        tracker.update_progress()


print("Cluster completed, saving results")
# After completing all clustering steps, repartition to 400 partitions
df = df.repartition(npartitions=400)

# Save as Parquet files
output_path = "/mnt/10tb_hdd/buckets_pca_dask"
df.to_parquet(output_path, engine='pyarrow', write_index=False)


# import dask.dataframe as dd
# import dask.array as da
# import numpy as np
# import os 

# input_folder = "/mnt/samsung_2tb/mixed_data/output/pca_transformed_results/output/*.parquet"

# df = dd.read_parquet(input_folder, engine="pyarrow")

# def get_percentiles(dataframe):
#     pca_array = dataframe.to_dask_array(lengths=True)
#     percentiles_to_compute = [i for i in np.arange(102, step=2)]
#     percentiles = da.percentile( pca_array, q=percentiles_to_compute, method='linear', internal_method='tdigest').compute()

#     return percentiles.tolist()

# def assign_bucket(part, name_pca_column, percentiles):
#     cluster_ids = np.digitize(part[name_pca_column], percentiles) - 1
#     cluster_ids = np.where(cluster_ids==50, 49, cluster_ids)
#     part['cluster_id'] = cluster_ids
#     return part

# pca1_percentiles = get_percentiles(df['PCA_1'])

# df = df.map_partitions(assign_bucket())


# for i in range(50): 
#     bucket = df[df['cluster_id'] == i]

#     # Convert 'PCA_1' column to a Dask array
#     pca2_array = df['PCA_2'].to_dask_array(lengths=True)

#     # Define the percentiles to compute
#     percentiles_to_compute = [i for i in np.arange(102, step=2)]

#     # Compute the percentiles using Dask's internal method
#     percentiles = da.percentile(
#         pca2_array, 
#         q=percentiles_to_compute, 
#         method='linear', 
#         internal_method='tdigest'  # or 'tdigest' for memory efficiency
#     ).compute()

#     def assign_bucket(part):
#         cluster_ids = np.digitize(part['PCA_1'], percentiles) - 1 
#         cluster_ids = np.where(cluster_ids==50, 49, cluster_ids )
#         part['cluster_id'] = cluster_ids
#         return part

#     df = df.map_partitions(assign_bucket)

#     for i in range(50):

