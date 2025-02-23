
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import concat_ws, udf
from pyspark.sql.functions import struct

def length_blocking_parallel(df, columns_to_use, *args):
    udf_length_int = udf(lambda x: len(str(x)), IntegerType())
    udf_length_str = udf(lambda x: str(len(str(x))), StringType())
    df = df.withColumn("value",struct(*(udf_length_int(df[column_name]) for column_name in columns_to_use)))
    df = df.withColumn("blocking_key", concat_ws("_", *(udf_length_str(df[column_name]) for column_name in columns_to_use)))
    return df