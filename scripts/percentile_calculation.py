import pandas as pd 
import numpy as np 
from tdigest import TDigest
import os 
from api.utils.helper_functions import TimeTracker, FileProgressTracker
import json

digest = TDigest()
input_folder = "/mnt/samsung_2tb/mixed_data/output/pca_transformed_results/output/"

with FileProgressTracker(description="Percentile Calculation", total_files=len(os.listdir(path=input_folder))) as tracker:
    percentile_dict = {} 
    for path in os.listdir(path=input_folder):
        try:
            df = pd.read_parquet(os.path.join(input_folder, path), engine='pyarrow')
        except:
            continue

        digest.batch_update(df['PCA_1'])
        del df 
        tracker.update_progress()

    step = 2 
    percentiles = [i for i in np.arange(stop=100, step=2)]

    for percentile in percentiles:
        percentile_dict[percentile] = digest.percentile(percentile)

print(percentile_dict)

with open('percentiles.txt', 'w') as file:
    file.write(json.dumps(percentile_dict))