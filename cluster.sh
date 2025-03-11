#!/bin/bash

for i in {0..49}; do
    echo "Starting iteration $i..."
    python test.py \
        --input_path "/mnt/samsung_2tb/test_enamine_clustering/cluster_id_1=${i}/" \
        --col PCA_2 \
        --new_cluster_col cluster_id_2 \
        --n_bins 50 \
        --output_path "/mnt/10tb_hdd/enamine_db_10db_clustered/cluster_pca_2/output_dataframe_cluster_id_1_${i}/"
    echo "Completed iteration $i."
done

echo "All iterations completed."
