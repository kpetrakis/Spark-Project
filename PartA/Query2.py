from pyspark import SparkContext
sc = SparkContext("local", "First App")

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("First App") \
    .getOrCreate()


from pyspark.sql.functions import *
# epilegoume tin Ellada kai 5 akoma xwres
# exw 2 diaforetika sinola listwn gia na fanei i diafora otan einai i den einai megisti i Ellada!
selected_countries = ['Greece','Belgium','Denmark', 'Croatia','Germany (until 1990 former territory of the FRG)','Cyprus']
# selected_countries = ['Greece','Belgium','Croatia','Cyprus','France','Hungary']

# kataskeuazw neo dataframe to opoio periexei mono tis epilegmenes xwres 
df_2 = df.filter(col('GEO/TIME').isin(selected_countries))
df_2.show()

# lista pou tha krata tis xronies pou i Ellada eixe to megisto arithmo dianiktereusewn 
years_greece_was_greatest = []
count = 0
# diatrexw gia kathe xronia plin tis stilis 'GEO/TIME'
for year in df_2.columns:
    if (year == 'GEO/TIME'):
        continue
#   taksinomw kathe stili me fthinousa diataksi kai eksagw tin xwra pou eixe to megisto
#   an auti itan i Ellada tote prosthetw ti xronia sti lista years_greece_was_greatest
    if (df_2.sort(df[year].desc()).collect()[0][0] =='Greece'):
        years_greece_was_greatest.append(year)
        count+=1 # borw na to kanw kai me to len tis listas!
    

# ektipwnw tis xronies pou i Ellada eixe to megisto arithmo dianiktereusewn kai poses itan autes i xronies
print(f"H Ellada eixe perissoteres episkepseis tis xronies: {years_greece_was_greatest}")
print(f"Sinolika h Ellada eixe perissotera arrivals gia {len(years_greece_was_greatest)} xronies")