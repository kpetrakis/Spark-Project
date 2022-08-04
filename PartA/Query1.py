from pyspark import SparkContext
sc = SparkContext("local", "First App")

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("First App") \
    .getOrCreate()


from pyspark.sql.functions import col, lit
# athroizoume tis stiles 2007 ews 2014 kai diairoume me to plithos tous
df.withColumn("mean_07-14",(col('2007')+col('2008')+col('2009')+col('2010')+col('2011')+col('2012')+col('2013')+col('2014'))/8).show()