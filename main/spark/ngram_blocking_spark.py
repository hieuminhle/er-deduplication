from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, collect_set, first, sha2, concat_ws, col, lit, when
from pyspark.sql.types import StringType, ArrayType, StructField, StructType
from pyspark.sql.types import ArrayType, StringType
from typing import Dict

def ngram_partition(iterator, n):
    for row in iterator:
        blocking_key = row['blocking_key']
        ngram_value = [blocking_key[i:i+n] for i in range(len(blocking_key)-n+1)]
        yield blocking_key, {'value': ngram_value, 'index': row['collect_list(index)']}


# dataframe version
def initial_ngram_parallel_df(spark, df, columns_to_use, n):

    if 'publication_venue' in columns_to_use:
        df = df.withColumn('publication_venue', F.when(df['publication_venue'].contains('sigmod'), 'sigmod').otherwise('vldb'))

    transform_author_names_udf = F.udf(lambda value: "".join([name[0] if len(name.split()) == 1 else name.split()[0][0] + name.split()[-1][0] for name in value.split()]), StringType())
    transform_paper_title_udf = F.udf(lambda value: "".join([word[0] for word in value.split()]), StringType())

    for column in columns_to_use:
        if column == 'author_names':
            df = df.withColumn(column, transform_author_names_udf(column))
        elif column == 'paper_title':
            df = df.withColumn(column, transform_paper_title_udf(column))
        elif column == 'year' or column == 'publication_venue':
            df = df.withColumn(column, F.col(column).cast(StringType()))

    df = df.withColumn('blocking_key', F.udf(lambda *args: ''.join(str(arg) for arg in args), StringType())(*columns_to_use))
    blocks = df.groupBy('blocking_key').agg(F.collect_list('index'))

    blocks = blocks.rdd.mapPartitions(lambda iterator: ngram_partition(iterator, n))
    blocks_dict = dict(blocks.collect())

    schema = StructType([
        StructField("blocking_key", StringType(), nullable=False),
        StructField("value", StringType(), nullable=False),
        StructField("index", StringType(), nullable=False)
    ])

    data_tuples = [(blocking_key, value['value'], value['index']) for blocking_key, value in blocks_dict.items()]

    blocks_df = spark.createDataFrame(data_tuples, schema=schema)
  
    return blocks_df



def n_gram_blocking(df, columns, n) :

    if 'publication_venue' in columns:
        df = df.withColumn('publication_venue', when(col('publication_venue').contains('sigmod'), 'sigmod').otherwise('vldb'))

    if 'year' in columns:
        df = df.withColumn('year', col('year').cast(StringType()))


    ngrams_udf = udf(lambda s, n: [tuple(s[i:i+n]) for i in range(len(s)-n+1)], ArrayType(StringType()))


    ngram_col_names = []
    for column in columns:
        ngram_col_name = f'ngram_values_{column}'
        df = df.withColumn(ngram_col_name, ngrams_udf(col(column), lit(n)))
        ngram_col_names.append(ngram_col_name)

    concatenated_ngrams_col = 'value'
    df = df.withColumn(concatenated_ngrams_col, concat_ws('_', *[col(name) for name in ngram_col_names]))

    hash_col_name = 'blocking_key'
    df = df.withColumn(hash_col_name, sha2(concatenated_ngrams_col, 256))


    blocks = df.groupBy(concatenated_ngrams_col) \
               .agg(first(hash_col_name).alias('blocking_key'), collect_set('index').alias('index'))
    blocks = blocks.select('blocking_key', concatenated_ngrams_col, 'index')
    return blocks
            
"""
def initial_ngram_parallel(path, columns_to_use, n):
    spark = SparkSession.builder.appName("Initialngram").getOrCreate()
    df = spark.read.csv(path, header=True, inferSchema=True)
    
    if 'publication_venue' in columns_to_use:
        df = df.withColumn('publication_venue', F.when(df['publication_venue'].contains('sigmod'), 'sigmod').otherwise('vldb'))

    transform_author_names_udf = F.udf(lambda value: "".join([name[0] if len(name.split()) == 1 else name.split()[0][0] + name.split()[-1][0] for name in value.split()]), StringType())
    transform_paper_title_udf = F.udf(lambda value: "".join([word[0] for word in value.split()]), StringType())

    for column in columns_to_use:
        if column == 'author_names':
            df = df.withColumn(column, transform_author_names_udf(column))
        elif column == 'paper_title':
            df = df.withColumn(column, transform_paper_title_udf(column))
        elif column == 'year' or column == 'publication_venue':
            df = df.withColumn(column, F.col(column).cast(StringType()))

    df = df.withColumn('blocking_key', F.udf(lambda *args: ''.join(str(arg) for arg in args), StringType())(*columns_to_use))
    blocks = df.groupBy('blocking_key').agg(F.collect_list('index'))

    blocks = blocks.rdd.mapPartitions(lambda iterator: ngram_partition(iterator, n))

    blocks_dict = dict(blocks.collect())

    spark.stop()
    return blocks_dict
"""


