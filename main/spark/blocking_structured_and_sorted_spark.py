from pyspark.sql import SparkSession, functions as F

def block_by_year_and_publisher(path, year_block, labels):
    spark = SparkSession.builder.appName("block_by_year_and_publisher").getOrCreate()
    df = spark.read.csv(path, header=True, inferSchema=True)

    year_ranges = []
    publishers = ['sigmod', 'vldb']

    for label in labels:
        publisher_blocks = []

        # Filter data within the year range
        intervals = df.filter((F.col('year') >= year_block[labels.index(label)]) & 
                              (F.col('year') < year_block[labels.index(label) + 1]))

        for publisher in publishers:
            publisher_block = intervals.filter(F.col('publication_venue').contains(publisher))
            if publisher_block.count() > 0:
                publisher_data = publisher_block.rdd.map(lambda row: row.asDict()).collect()
                publisher_blocks.append(publisher_data)
        year_ranges.extend(publisher_blocks)

    return year_ranges


