# i proti xronia 1990
prev_year = df.columns[1]

# gia kathe xronia apo to 1995 kai meta
for current_year in df.columns[2:]:
# prosthetw sto dataframe ti antistoixi stili me ti posostiaia metavoli
# gia to pososto pairnw ti diafora (metagenesteri-prohgoumeni) kai diairw me ti timi tis metagenesteris
    df = df.withColumn(f'posostiaia_metavoli_{prev_year}-{current_year}',(col(current_year)-col(prev_year))/col(current_year)*100)
#   pleon i prohgoumeni xronia ginetai isi me ti trexousa (apo 1990-1995 pame se 1995-2000 kok)
    prev_year = current_year


# olo to dataframe
df.show()
# mono oi zitoumenes stiles gia na fainontai kalitera
df.select(col('posostiaia_metavoli_1990-1995'),col('posostiaia_metavoli_1995-2000'),col('posostiaia_metavoli_2000-2005'),col('posostiaia_metavoli_2005-2010'),col('posostiaia_metavoli_2010-2014'),col('posostiaia_metavoli_2014-2015')).show()

# to idio xwris ti xrisi vroxou
# df = df.withColumn('posostiaia_metavoli_90-95',(col('1995')-col('1990'))/col('1995')*100)
# df = df.withColumn('posostiaia_metavoli_95-2000', (col('2000')-col('1995'))/col('2000')*100)
# df = df.withColumn('posostiaia_metavoli_2000-2005', (col('2005')-col('2000'))/col('2005')*100)
# df = df.withColumn('posostiaia_metavoli_2005-2010', (col('2010')-col('2005'))/col('2010')*100)
# df = df.withColumn('posostiaia_metavoli_2010-2014', (col('2014')-col('2010'))/col('2014')*100)
# df = df.withColumn('posostiaia_metavoli_2014-2015', (col('2015')-col('2014'))/col('2015')*100)