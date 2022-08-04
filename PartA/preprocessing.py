import pyspark
from pyspark import SparkContext
sc = SparkContext("local", "First App")

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("First App") \
    .getOrCreate()

import pandas as pd
# ta onomata twn stilwn einai stin grammi 8
tour_data = pd.read_excel("tour_occ_ninat.xlsx",header=8)
tour_data.head()
# metatrepoume to arxeio se .csv - xwris tin arithmisi twn grammwn
tour_data.to_csv("tour_occ_ninat.csv",index=False)

# kataskeuazw to dataframe apo to csv arxeio sto pyspark
df = spark.read.option("header","true").load("tour_occ_ninat.csv",format='csv')
df.show()

df.printSchema()

# allazoume ton tipo stilwn apo string se int
# allazw oles tis stiles me tin kathe xronia - dld ektos apo tin prwti me tis xwres
for column in df.columns:
# for column in df.columns[1:]:
    if column == "GEO/TIME":
        continue
    else:
        df = df.withColumn(column,df[column].cast("int"))

# neo Schema
df.printSchema()

# antikathistw tis ellipeis times me to 0
df = df.fillna(0)
df.show()