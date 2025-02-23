import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lower, upper, reverse
from pyspark.sql.types import StringType, FloatType
#from similarity_spark import jaccard_similarity_wrapper as jaccard_similarity
from pyspark.sql.functions import collect_list, when
from itertools import combinations
import hash_blocking_spark as hash
import length_blocking_spark as length
import ngram_blocking_spark as ngram
from Levenshtein import ratio
import matchers_spark as matchers #apply_similarity_sorted
import time
import re

baselines_folder = r"./baselines"
csv_folder = r"./CSV-files"
dblp_csv_path = r"/dblp_stem.csv"
acm_csv_path = r"/acm_stem.csv"

# Initialize Spark session
spark = SparkSession.builder.appName("EntityResolution").getOrCreate()
# spark.sparkContext._jvm.System.gc()

@udf(FloatType())
def jaccard_similarity_wrapper(one, two):
    if isinstance(one, list) and isinstance(two, list):
        try:
            return jaccard_similarity_ngrams(one, two)
        except:
            return jaccard_similarity(one, two)
    return jaccard_similarity(one, two)

@udf(FloatType())
def jaccard_similarity(set1, set2):
    if not isinstance(set1, set):
        set1 = set(set1)
    if not isinstance(set2, set):
        set2 = set(set2)
    
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    
    return intersection / union if union > 0 else 0

# when the values are ngrams instead of string
@udf(FloatType())
def jaccard_similarity_ngrams(ngrams1, ngrams2):
    set1 = set(map(tuple, ngrams1))
    set2 = set(map(tuple, ngrams2))

    intersection = len(set1.intersection(set2))
    union = len(set1) + len(set2) - intersection   

    if union == 0:
        return 0.0  
    
    return intersection / union

@udf(StringType())
def title(entity: str) -> str:
    return entity.title()

# @udf(StringType())
# def upper(entity: str) -> str:
#     return entity.upper()

# @udf(StringType())
# def lower(entity: str) -> str:
#     return entity.lower()

@udf(StringType())
def alphanumerical(entity: str) -> str:
    return ''.join([char for char in entity if char.isalnum()])

# @udf(StringType())
# def reverse(entity: str) -> str:
#     return entity[::-1]

# Load CSV files into DataFrames
# column names paper_title, author_names, year, publication_venue, index
dblp_df = spark.read.csv(csv_folder+dblp_csv_path, header=True, inferSchema=True)
acm_df = spark.read.csv(csv_folder+acm_csv_path, header=True, inferSchema=True)
dataframes = [dblp_df, acm_df]

for df in dataframes:
    df = df.withColumn('publication_venue', when(df['publication_venue'].contains('sigmod'), 'sigmod').otherwise('vldb'))

dblp_blocked_df = matchers.blocking_by_year_and_publisher_column_(dblp_df, None)
acm_blocked_df = matchers.blocking_by_year_and_publisher_column_(acm_df, None)

dblp_df_datasets = {"full": dblp_df}
dblp_df_datasets.update({group_key:dblp_blocked_df.filter(col("blocking_key") == group_key) for group_key in dblp_blocked_df.select("blocking_key").distinct().rdd.flatMap(lambda x: x).collect()})
acm_df_datasets = {"full": acm_df}
acm_df_datasets.update({group_key:acm_blocked_df.filter(col("blocking_key") == group_key) for group_key in acm_blocked_df.select("blocking_key").distinct().rdd.flatMap(lambda x: x).collect()})

augementations = [upper, lower, title, alphanumerical, reverse]

# Preprocessing: Apply augmentations to both datasets
for comb in combinations(augementations, 2):
    for column, dtype in df.dtypes:
        if column != "id" and dtype == "string":
            dblp_result_df = dblp_df.withColumn(column, comb[1](comb[0](col(column))))
            acm_result_df = acm_df.withColumn(column, comb[1](comb[0](col(column))))
    dblp_df_datasets.update({f"{comb[0].__name__}_{comb[1].__name__}": dblp_result_df})
    acm_df_datasets.update({f"{comb[0].__name__}_{comb[1].__name__}": acm_result_df})

columns = dblp_df.columns
columns.remove("index")

blocking_functions = [#length.length_blocking_parallel,\
    hash.initial_hash_parallel_df\
        #, ngram.initial_ngram_parallel_df
        ] # Add your blocking functions here
baselines = {0.7:{"jac":spark.read.csv(baselines_folder+r"/base_7_jac_stem.csv", header=True, inferSchema=True),
                  #"len": spark.read.csv(baselines_folder+r"\base_7_l_stem.csv", header=True, inferSchema=True)
                  },
#                "lev":spark.read.csv(baselines_folder+r"\base_7_lev_stem.csv", header=True, inferSchema=True)},
             0.85:{"jac":spark.read.csv(baselines_folder+r"/base_85_jac_stem.csv", header=True, inferSchema=True),
                  #"len": spark.read.csv(baselines_folder+r"\base_7_l_stem.csv", header=True, inferSchema=True)
                  }}#,
                #"lev":spark.read.csv(baselines_folder+r"\base_7_lev_stem.csv", header=True, inferSchema=True)}}

spark.udf.register("jaccard_similarity", jaccard_similarity)
similarity_functions = {"jac":jaccard_similarity,} #[ratio] # Add your similarity functions here

# Blocking: Apply blocking to both datasets
for r in range(1, len(columns) + 1):
    for comb in combinations(columns, r):
        for blocking in blocking_functions:
            for key in dblp_df_datasets:
                grouped_dfs = []
                if key in acm_df_datasets:
                    for df in [dblp_df_datasets[key], acm_df_datasets[key]]:
                        start_time = time.time()
                        blocked_df = blocking(df, comb, spark)
                        #grouped_df = blocked_df.groupBy("blocking_key")
                        grouped_df = blocked_df
                        end_time = time.time()
                        execution_time = end_time - start_time
                        #count = grouped_df.count().count()
                        count = acm_blocked_df.select("blocking_key").distinct().count()
                        print(f"Combination {comb}\nwith blocking method '{blocking.__name__}'\ntook {execution_time} seconds and resulted in {count} groups on the {key} dataset.")
                        grouped_dfs.append(grouped_df)
                    for threshold in baselines:
                        for similarity_function_key in baselines[threshold]:
                            print(f"Applying {similarity_functions[similarity_function_key].__name__} baseline with threshold {threshold} on {key} dataset.")
                            #matches = matchers.apply_similarity_sorted(grouped_dfs[0], grouped_dfs[1], threshold, similarity_functions[similarity_function_key], ["value"])
                            matches = matchers.apply_similarity_blocks_spark(grouped_dfs[0], grouped_dfs[1], threshold, similarity_functions[similarity_function_key])                            
                            print(f"Found {len(matches)} matches.")


spark.stop()
exit()

a = hash.initial_hash_parallel(acm_df, selected_columns)

# Check the number of groups when grouping by "initials"
num_init_groups = a.groupBy("initials").count().count()
num_hash_groups = a.groupBy("blocking_key").count().count()
grouped_data = a.groupBy("blocking_key").agg(collect_list("index").alias("index_list"))

num_pairs = 0

for row in grouped_data.collect():
    index_list = row["index_list"]
    # Berechne die kombinatorische Vielfalt
    combinations_count = len(list(combinations(index_list, 2)))
    num_pairs += combinations_count

print("Number of pairs: ", num_pairs)

# Print the number of groups
print("Number of groups: ", num_init_groups)
print("Number of groups: ", num_hash_groups)

# Define a user-defined function (UDF) for similarity calculation
def similarity_udf(title1, title2):
    return jaccard_similarity(title1, title2)  # Change this to your desired similarity function

# Register the UDF
similarity_udf_spark = udf(similarity_udf, StringType())

# Matching: Apply similarity function to determine matches
matches = dblp_blocked.crossJoin(acm_blocked)
matches = matches.withColumn("similarity", similarity_udf_spark(matches["titles"], matches["titles"]))

# Filter matches based on similarity threshold (e.g., 0.7)
matched_pairs = matches.filter(col("similarity") > 0.7).select("year", "similarity")

# Write matched pairs to a CSV file
matched_pairs.write.csv("MatchedEntities.csv", header=True, mode="overwrite")

# Baseline: Apply similarity function on all pairs of the datasets
all_pairs = dblp_df.crossJoin(acm_df)
all_pairs = all_pairs.withColumn("similarity", similarity_udf_spark(all_pairs["title"], all_pairs["title"]))

# Calculate precision, recall, and F-measure
true_positives = matched_pairs.count()
false_positives = all_pairs.filter((col("similarity") > 0.7) & ~col("year_x").isNull()).count()
false_negatives = all_pairs.filter((col("similarity") <= 0.7) & ~col("year_x").isNull()).count()

precision = true_positives / (true_positives + false_positives)
recall = true_positives / (true_positives + false_negatives)
f_measure = 2 * (precision * recall) / (precision + recall)

# Print evaluation similarity_functions
print("Precision: {}".format(precision))
print("Recall: {}".format(recall))
print("F-measure: {}".format(f_measure))

# Clustering: Group together all identified matches in clusters
# Your clustering logic goes here

# Resolve unmatched entities and write them back to disk
# Your resolution logic goes here

# Replicate each dataset 1 to 10 times with minor modifications
# Plot the resulting execution time for each replication factor
# Your replication and plotting logic goes here

# Stop Spark session
spark.stop()
