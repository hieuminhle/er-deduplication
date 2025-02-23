
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, when, udf
import hashlib
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, StructField, StructType


func = {'author_names':udf(lambda entry: "".join(name[0] for name in entry.split()), StringType()),
        'year':udf(lambda entry: str(entry), StringType()),
        'publication_venue':udf(lambda entry: str(entry), StringType()),
        'paper_title':udf(lambda entry: "".join(word[0] for word in entry.split()), StringType())}

from pyspark.sql.functions import col
from pyspark.sql import functions as F

def hash_partition(iterator):
    for row in iterator:
        blocking_key = row['blocking_key']
        hash_value = hashlib.md5(blocking_key.encode()).hexdigest()
        yield blocking_key, {'value': hash_value, 'index': row['collect_list(index)']}

def initial_hash_parallel(df, columns_to_use, *args):
    # Neue Spalte hinzufügen
    df = df.withColumn("blocking_key", concat_ws("", *(func[column_name](df[column_name]) for column_name in columns_to_use)))
    df = df.withColumn("value",udf(lambda entry: hashlib.md5(entry.encode()).hexdigest(), StringType())(df["blocking_key"]))


    return df

def hash_partition(iterator):
    for row in iterator:
        blocking_key = row['blocking_key']
        hash_value = hashlib.md5(blocking_key.encode()).hexdigest()
        yield blocking_key, {'value': hash_value, 'index': row['collect_list(index)']}

def initial_hash_parallel_df(spark, df, columns_to_use):
   
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

    blocks = blocks.rdd.mapPartitions(hash_partition)

    blocks_dict = dict(blocks.collect())
    schema = StructType([
        StructField("blocking_key", StringType(), nullable=False),
        StructField("value", StringType(), nullable=False),
        StructField("index", StringType(), nullable=False)
    ])

    data_tuples = [(blocking_key, value['value'], value['index']) for blocking_key, value in blocks_dict.items()]
    blocks_df = spark.createDataFrame(data_tuples, schema=schema)
    return blocks_df



def hash_blocking_spark(spark, df, columns):
    transform_hash_udf = F.udf(lambda row: hashlib.md5(''.join(map(str, row)).encode()).hexdigest(), StringType())

    # Apply the UDF to create the hash key column
    df = df.withColumn('hash_key', transform_hash_udf(F.struct(columns)))

    # Group by 'hash_key' and collect the 'index' values into a list
    blocks = df.groupby('hash_key').agg(F.collect_list('index')).rdd \
        .map(lambda row: {'blocking_key': row['hash_key'], 'value': row['hash_key'], 'index': row['collect_list(index)']}).collect()

    # Convert the result to a dictionary
    blocks_dict = {block['blocking_key']: {'value': block['value'], 'index': block['index']} for block in blocks}

    schema = StructType([
        StructField("blocking_key", StringType(), nullable=False),
        StructField("value", StringType(), nullable=False),
        StructField("index", StringType(), nullable=False)
    ])
    data_tuples = [(blocking_key, value['value'], value['index']) for blocking_key, value in blocks_dict.items()]

    # to dataframe
    blocks_df = spark.createDataFrame(data_tuples, schema=schema)
    return blocks_df

"""
def initial_hash_parallel(path, columns_to_use):
    spark = SparkSession.builder.appName("InitialHash").getOrCreate()
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

    blocks = blocks.rdd.mapPartitions(hash_partition)

    blocks_dict = dict(blocks.collect())

    spark.stop()
    return blocks_dict
"""