# import findspark
# findspark.init()

import pyspark

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("First App") \
    .getOrCreate()

df = spark.read.option("header","true").load("tour_occ_ninat.csv",format='csv')
df.show()
df.printSchema()