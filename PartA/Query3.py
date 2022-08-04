from pyspark import SparkContext
sc = SparkContext("local", "First App")

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("First App") \
    .getOrCreate()

from pyspark.sql.functions import *
# apothikeuw sti metavliti max_list to Row pou periexei to megisto kathe xronias
max_list = df.select(*(max(c) for c in df.columns[1:])).collect()[0]
iteration = 0
# diatrexw gia kathe zeugari xronias-megistou ekeinis tis xronias
for max_of_the_year,xronia in zip(max_list,df.columns[1:]):
#  eksagw tin poli pou eixe to megisto tin xronia pou vrisketai kathe fora o vroxos
    max_city_of_the_year = df.select(col('GEO/TIME').alias(f'maxCity_{xronia}')).filter(df[xronia]==max_of_the_year).collect()[0][0]
#     poli_df =  df.select(col('GEO/TIME').alias(f'maxCity_{xronia}')).filter(df[xronia]==max_of_the_year).collect()

#   sto prwto perasma dimiourgo to neo dataframe
    if iteration==0:
        new_df = df.select(col('GEO/TIME').alias(f'maxCity_{xronia}')).filter(df[xronia]==max_of_the_year)
#   sta ipoloipa perasmata afou exei dimiourgithei to dataframe, prosthetw apla tin antistoixi stili sto dataframe
    else:
#       i timi tis nea stillis einai i xwra pou eixe to megisto, kai xrisimopoiw san literal ti timi tis max_city_of_the_year
        new_df = new_df.withColumn(f'maxCity_{xronia}',lit(max_city_of_the_year))

#         new_df = new_df.withColumn(f'maxCity_{xronia}',lit(poli_df[0][0]))
    iteration+=1

new_df.show()