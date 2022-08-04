from pyspark import SparkContext
sc = SparkContext("local", "First App")

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("First App") \
    .getOrCreate()

import pandas as pd
# ta onomata stilwn vriskontai sti prwti grammi 1
tour_data = pd.read_excel("international_tourist_arrivals.xlsx",header=1)
tour_data.head()
# metatrwpw to arxeio se .csv xwris tin arithmisi twn grammw
tour_data.to_csv("international_tourist_arrivals.csv",index=False)

# kataskeuazw to dataframe apo to csv arxeio sto pyspark
df = spark.read.option("header","true").load("international_tourist_arrivals.csv",format='csv')
# metanomozw tin prwti stili pou den periexei onoma!
df = df.withColumnRenamed('Unnamed: 0','Region')
df.show()

# metatrepw tis stiles me tis xronies apo String se float
for column in df.columns[1:]:
    df = df.withColumn(column,df[column].cast("float"))

df.printSchema()

# antikathistw PROSWRINA tis ellipeis times me to 0
df = df.fillna(0)
df.show()