from pyspark.sql.functions import concat_ws, udf, col, lit
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from typing import List, Callable
from pyspark.sql import DataFrame
import time
from itertools import product

# UDF (User Defined Function) erstellen und Liste übergeben
group_years_udf = udf(lambda year, year_block, labels: labels[len([y for y in year_block if y <= year])-1], returnType=StringType())

def blocking_by_year_and_publisher_column_(df, columns_to_use, labels= ["1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004"], year_block=[1995,1996,1997, 1998, 1999,2000,2001, 2002, 2003, 2004,2005]):
    # Index-Spalte basierend auf der "year"-Spalte erstellen
    df = df.withColumn("blocking_key", concat_ws("-", group_years_udf(df["year"], lit(year_block), lit(labels)), df["publication_venue"]))
    return df

def apply_similarity_sorted(blocks1: DataFrame, blocks2: DataFrame, threshold: float, similarity_function: Callable, indices: List[str]):
    # blocks1 = blocks1.withColumn("value", col("value").cast(StringType()))
    # blocks2 = blocks2.withColumn("value", col("value").cast(StringType()))

    #joined_blocks = blocks1.alias("block1").crossJoin(blocks2.alias("block2"))
    joined_blocks = blocks1.alias("block1").join(blocks2.alias("block2"), blocks1["blocking_key"] == blocks2["blocking_key"])
    matched_blocks = joined_blocks.filter(similarity_function(col("block1.value"), col("block2.value")) > threshold)
    
    # matched_blocks.show()
    matched_blocks_list = [(row["index"], row["index"]) for row in matched_blocks.collect()]

    return list(set(matched_blocks_list))

def apply_similarity_blocks_spark(df1, df2, threshold, similarity_function, *args, **kwargs):
    start_time = time.time()   
 
    similarity_udf = udf(lambda set1, set2: str(similarity_function(set(set1), set(set2))), StringType())

    joined_blocks = df1.alias("block1").crossJoin(df2.alias("block2"))
    similar_pairs_df = joined_blocks.where(
        (col("block1.value") == col("block2.value"))
    ).select(
        col("block1.index").alias("index1"),
        col("block2.index").alias("index2"),
        similarity_udf(col("block1.value"), col("block2.value")).alias("similarity")
    )

    similar_pairs_df = similar_pairs_df.filter(col("similarity") >= threshold)
    similar_pairs = similar_pairs_df.collect()

    processed_data = []
   
    for row in similar_pairs:
        index1_values = row['index1'][1:-1].split(', ')
        index2_values = row['index2'][1:-1].split(', ')

        if len(index1_values) > 1 or len(index2_values) > 1:
            index_combinations = list(product(index1_values, index2_values))
            for combination in index_combinations:
                processed_data.append((combination[0], combination[1]))
        else:
            
            processed_data.append(row)
            processed_data = list(set(processed_data))

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Processing time: {elapsed_time} seconds. Number of index combinations: {len(processed_data)}")

    return processed_data
# for ngram_blocking cause is a list not a dict (similiar_pairs)
def apply_ngram_blocks_spark(df1, df2, threshold, similarity_function):
    start_time = time.time()
 
    similarity_udf = udf(lambda set1, set2: str(similarity_function(set(set1), set(set2))), StringType())

    joined_blocks = df1.alias("block1").crossJoin(df2.alias("block2"))
    similar_pairs_df = joined_blocks.where(
        (col("block1.value") == col("block2.value"))
    ).select(
        col("block1.index").alias("index1"),
        col("block2.index").alias("index2"),
        similarity_udf(col("block1.value"), col("block2.value")).alias("similarity")
    )

    similar_pairs_df = similar_pairs_df.filter(col("similarity") >= threshold)
    similar_pairs = similar_pairs_df.collect()

    processed_data = []
   
    for row in similar_pairs:
   
        index1_values = row.index1
        index2_values = row.index2
        index1_values = [value for value in index1_values]
        index2_values = [value for value in index2_values]

        if len(index1_values) > 1 or len(index2_values) > 1:
            index_combinations = list(product(index1_values, index2_values))
            for combination in index_combinations:
                processed_data.append((combination[0], combination[1]))
        else:
            processed_data.append(row)
        
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Processing time: {elapsed_time} seconds. Number of index combinations: {len(processed_data)}")

    return processed_data