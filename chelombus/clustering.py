import sys
import os 
import logging
from pyspark.sql import SparkSession
from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import ntile
from pyspark.sql.functions import avg, sqrt, pow, col, row_number, broadcast

#TODO: Complete pipeline for constructing the Apache Dataframe

class ClusterMethod():
    def __init__(self, bin_size: list, output_path: str):
        self.spark = SparkSession.builder \
            .appName("HierarchicalBinning") \
            .config("spark.driver.memory", "64g") \
            .config("spark.executor.memory", "64g") \
            .getOrCreate()
        self.output_path = output_path 

        self.bin_size = bin_size

    def _get_count(self):
        """
        Get number of samples in the Apache Spark dataframe
        """
        pass

    def spark_constructor(self, file_path):
        """
        Method to create a Apache Spark database to perform the clustering of large datastets (i.e. that cannot be allocated entirely in memory)
        param: file_path: the path to the folder where all the parquet files are stored
        """
        database= self.spark.read.parquet(f"{file_path}/*")
        print(database.count())


    def create_clusters(self, dataframe):
        """
        Method to create the clusters from Spark dataframe. The idea is to cluster by sorting the first PCA dimension, then creating n-size bins on that dimension
        And then on each bin, sort by the second PCA dimension, create n-size bins. And so on for every dimension we have.
        At the end we will have n**(number of PCA dimensions) equally sized clusters.
        """

        # Define the percentiles for 25 bins
        percentiles_pca_1= [i / self.bin_size[0] for i in range(1, self.bin_size[0])]

        # Compute approximate quantiles for PCA_1
        pca1_thresholds = dataframe.approxQuantile("PCA_1", percentiles_pca_1, 0.01)

        # Create splits for Bucketizer
        pca1_splits = [-float("inf")] + pca1_thresholds + [float("inf")]

        # Initialize Bucketizer
        bucketizer_pca1 = Bucketizer(
            splits=pca1_splits,
            inputCol="PCA_1",
            outputCol="bin_PCA1_temp"
        )

        # Transform the DataFrame
        dataframe = bucketizer_pca1.transform(dataframe)

        # Convert bin_PCA1_temp to IntegerType and adjust to start from 0
        dataframe = dataframe.withColumn("bin_PCA1", col("bin_PCA1_temp").cast("integer"))

        # Drop the temporary column
        dataframe = dataframe.drop("bin_PCA1_temp")

        # Create a window specification for PCA_2 within each bin_PCA1
        window_pca2 = Window.partitionBy("bin_PCA1").orderBy("PCA_2")

        # Assign bins for PCA_2, starting from 0
        dataframe = dataframe.withColumn("bin_PCA2", ntile(self.bin_size[1]).over(window_pca2) - 1)

        # Create a window specification for PCA_3 within each bin_PCA1 and bin_PCA2
        window_pca3 = Window.partitionBy("bin_PCA1", "bin_PCA2").orderBy("PCA_3")

        # Assign bins for PCA_3, starting from 0
        dataframe = dataframe.withColumn("bin_PCA3", ntile(self.bin_size[2]).over(window_pca3) - 1)

        # Combine bin columns to form a cluster identifier
        dataframe = dataframe.withColumn("cluster_id", concat_ws("_", "bin_PCA1", "bin_PCA2", "bin_PCA3"))

        dataframe = dataframe.select("smiles", "PCA_1", "PCA_2", "PCA_3", "cluster_id")

        # return new dataframe
        return dataframe 

    def save_first_dimension_cluster(self, dataframe):
        """
        Save into a csv all the points that belongs to each PCA1 cluster. 
        e.g. cluster_10.csv will have all compounds with cluster_id == 19_*_*
        """
        for i in range(self.bin_size[0]):
            df_cluster1 = dataframe.filter(col("bin_PCA1") == i)
            df_cluster1_pd = df_cluster1.toPandas()
            df_cluster1_pd.to_csv(f"{self.output_path}/cluster_{i}.csv", index=False)

    def get_representatives(self, dataframe):
        """
        Method to get compounds that act as representatives of the cluster. 
        This will constitute the dataframe that is used for the first TMAP
        The representative is the molecule that is the closest to the average value for every PCA dimension
        """

        # Step 1: Compute average PCA values for each cluster
        cluster_centers = dataframe.groupBy("cluster_id").agg(
            avg("PCA_1").alias("avg_PCA_1"),
            avg("PCA_2").alias("avg_PCA_2"),
            avg("PCA_3").alias("avg_PCA_3")
        ).cache()

        # Step 2: Join the cluster centers back to the original DataFrame
        df_with_centers = dataframe.join(broadcast(cluster_centers), on="cluster_id")

        # Step 3: Calculate the Euclidean distance to the cluster center
        df_with_distance = df_with_centers.withColumn(
            "distance",
            sqrt(
                pow(col("PCA_1") - col("avg_PCA_1"), 2) +
                pow(col("PCA_2") - col("avg_PCA_2"), 2) +
                pow(col("PCA_3") - col("avg_PCA_3"), 2)
            )
        )

        # Step 4: Find the closest molecule in each cluster
        window_spec = Window.partitionBy("cluster_id").orderBy("distance")

        df_with_rank = df_with_distance.withColumn(
            "rn",
            row_number().over(window_spec)
        )

        closest_molecules = df_with_rank.filter(col("rn") == 1).select(
            "smiles",
            "PCA_1",
            "PCA_2",
            "PCA_3",
            "cluster_id"
        )

        # Step 5: Create the new_cluster_dataframe
        new_cluster_dataframe = closest_molecules

        cluster_representatives = new_cluster_dataframe.toPandas()

        cluster_representatives.to_csv(f'{self.output_path}/cluster_representatives.csv', index=False)


# clustering_class.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, concat_ws
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import QuantileDiscretizer

class EfficientClustering:
    def __init__(self, input_path, output_path, spark_session=None):
        self.input_path = input_path
        self.output_path = output_path
        self.spark = spark_session or SparkSession.builder \
            .appName("EfficientClustering") \
            .config("spark.driver.memory", "60g") \
            .config("spark.sql.shuffle.partitions", "1000") \
            .getOrCreate()
        self.df = None

    def load_data(self):
        self.df = self.spark.read.parquet(self.input_path)

    def assign_pca1_buckets(self):
        discretizer_pca1 = QuantileDiscretizer(
            numBuckets=50, inputCol="pca_1", outputCol="cluster_pca1", relativeError=0.01
        )
        self.df = discretizer_pca1.fit(self.df).transform(self.df)

    def assign_pca2_buckets(self):
        cluster_pca1_values = [row['cluster_pca1'] for row in self.df.select("cluster_pca1").distinct().collect()]
        cluster_pca2_quantiles = {}
        for cluster_pca1_value in cluster_pca1_values:
            df_cluster = self.df.filter(self.df.cluster_pca1 == cluster_pca1_value)
            quantiles_pca2 = df_cluster.approxQuantile("pca_2", [i / 50.0 for i in range(1, 50)], 0.1)
            cluster_pca2_quantiles[cluster_pca1_value] = quantiles_pca2

        bc_cluster_pca2_quantiles = self.spark.sparkContext.broadcast(cluster_pca2_quantiles)

        def assign_cluster_pca2(cluster_pca1_value, pca_2_value):
            quantiles = bc_cluster_pca2_quantiles.value[cluster_pca1_value]
            for i, q in enumerate(quantiles):
                if pca_2_value <= q:
                    return i
            return 49

        assign_cluster_pca2_udf = udf(assign_cluster_pca2, IntegerType())
        self.df = self.df.withColumn("cluster_pca2", assign_cluster_pca2_udf(col("cluster_pca1"), col("pca_2")))

    def assign_pca3_buckets(self):
        cluster_pca1_pca2_values = self.df.select("cluster_pca1", "cluster_pca2").distinct().collect()
        cluster_pca3_quantiles = {}
        for row in cluster_pca1_pca2_values:
            cluster_pca1_value = row['cluster_pca1']
            cluster_pca2_value = row['cluster_pca2']
            df_cluster = self.df.filter(
                (self.df.cluster_pca1 == cluster_pca1_value) & (self.df.cluster_pca2 == cluster_pca2_value)
            )
            quantiles_pca3 = df_cluster.approxQuantile("pca_3", [i / 50.0 for i in range(1, 50)], 0.1)
            cluster_pca3_quantiles[(cluster_pca1_value, cluster_pca2_value)] = quantiles_pca3

        bc_cluster_pca3_quantiles = self.spark.sparkContext.broadcast(cluster_pca3_quantiles)

        def assign_cluster_pca3(cluster_pca1_value, cluster_pca2_value, pca_3_value):
            quantiles = bc_cluster_pca3_quantiles.value.get((cluster_pca1_value, cluster_pca2_value), [])
            for i, q in enumerate(quantiles):
                if pca_3_value <= q:
                    return i
            return 49

        assign_cluster_pca3_udf = udf(assign_cluster_pca3, IntegerType())
        self.df = self.df.withColumn(
            "cluster_pca3",
            assign_cluster_pca3_udf(col("cluster_pca1"), col("cluster_pca2"), col("pca_3"))
        )

    def create_cluster_id(self):
        self.df = self.df.withColumn(
            "cluster_id",
            concat_ws("_", col("cluster_pca1").cast("string"), col("cluster_pca2").cast("string"), col("cluster_pca3").cast("string"))
        )

    def select_columns(self):
        self.df = self.df.select("smiles", "pca_1", "pca_2", "pca_3", "cluster_id")

    def save_results(self):
        self.df.coalesce(10).write.mode("overwrite").parquet(self.output_path)

    def run(self):
        self.load_data()
        self.assign_pca1_buckets()
        self.assign_pca2_buckets()
        self.assign_pca3_buckets()
        self.create_cluster_id()
        self.select_columns()
        self.save_results()
        self.spark.stop()

# Example usage:
if __name__ == "__main__":
    input_path = "path_to_your_parquet_files"
    output_path = "path_to_output_directory"

    clustering = EfficientClustering(input_path, output_path)
    clustering.run()
