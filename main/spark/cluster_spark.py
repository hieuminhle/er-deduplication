from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def build_clusters_parallel(matched_entities):
    spark = SparkSession.builder \
        .appName("ClusterBuilder") \
        .getOrCreate()
    matched_entities_df = spark.createDataFrame(matched_entities, ["entity1", "entity2"])
    clusters = []

    for pair in matched_entities_df.collect():
        entity1, entity2 = pair
        related_clusters = []
        for cluster in clusters:
            if entity1 in cluster or entity2 in cluster:
                related_clusters.append(cluster)
        if related_clusters:
            merged_cluster = set()
            for related_cluster in related_clusters:
                merged_cluster.update(related_cluster)
                clusters.remove(related_cluster)
            merged_cluster.update(pair)
            clusters.append(merged_cluster)
        else:
            clusters.append(set(pair))

    spark.stop()
    return clusters




