from pyspark import SparkContext
sc = SparkContext("local", "First App")

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("First App") \
    .getOrCreate()

from pyspark.sql.functions import *
# dimiourgw ti stili year_min_was_found i opoia krata ti xronia pou vrethike to elaxisto
# kathe xronia tin sigkrinw me oles tis ipoloipes kai otan auti exei to mikrotero arithmo dianiktereusewn
# xrisimopoiw ti timi tiw sti stili year_min_was_found
df.withColumn('year_min_was_found',
   when((col('2006') < col('2007')) & (col('2006')<col('2008')) & (col('2006')<col('2009')) & (col('2006')<col('2010')) & (col('2006')<col('2011')) & (col('2006')<col('2012')) & (col('2006')<col('2013')) & (col('2006')<col('2014')) & (col('2006')<col('2015')), '2006').
   when((col('2007') < col('2006')) & (col('2007')<col('2008')) & (col('2007')<col('2009')) & (col('2007')<col('2010')) & (col('2007')<col('2011')) & (col('2007')<col('2012')) & (col('2007')<col('2013')) & (col('2007')<col('2014')) & (col('2007')<col('2015')),'2007').
   when((col('2008') < col('2006')) & (col('2008')<col('2007')) & (col('2008')<col('2009')) & (col('2008')<col('2010')) & (col('2008')<col('2011')) & (col('2008')<col('2012')) & (col('2008')<col('2013')) & (col('2008')<col('2014')) & (col('2008')<col('2015')),'2008').
   when((col('2009') < col('2006')) & (col('2009')<col('2007')) & (col('2009')<col('2008')) & (col('2009')<col('2010')) & (col('2009')<col('2011')) & (col('2009')<col('2012')) & (col('2009')<col('2013')) & (col('2009')<col('2014')) & (col('2009')<col('2015')),'2009').  
   when((col('2010') < col('2006')) & (col('2010')<col('2007')) & (col('2010')<col('2008')) & (col('2010')<col('2009')) & (col('2010')<col('2011')) & (col('2010')<col('2012')) & (col('2010')<col('2013')) & (col('2010')<col('2014')) & (col('2010')<col('2015')),'2010').
   when((col('2011') < col('2006')) & (col('2011')<col('2007')) & (col('2011')<col('2008')) & (col('2011')<col('2009')) & (col('2011')<col('2010')) & (col('2011')<col('2012')) & (col('2011')<col('2013')) & (col('2011')<col('2014')) & (col('2011')<col('2015')),'2011').
   when((col('2012') < col('2006')) & (col('2012')<col('2007')) & (col('2012')<col('2008')) & (col('2012')<col('2009')) & (col('2012')<col('2010')) & (col('2012')<col('2011')) & (col('2012')<col('2013')) & (col('2012')<col('2014')) & (col('2012')<col('2015')),'2012').
   when((col('2013') < col('2006')) & (col('2013')<col('2007')) & (col('2013')<col('2008')) & (col('2013')<col('2009')) & (col('2013')<col('2010')) & (col('2013')<col('2011')) & (col('2013')<col('2012')) & (col('2013')<col('2014')) & (col('2013')<col('2015')),'2013').
   when((col('2014') < col('2006')) & (col('2014')<col('2007')) & (col('2014')<col('2008')) & (col('2014')<col('2009')) & (col('2014')<col('2010')) & (col('2014')<col('2011')) & (col('2014')<col('2012')) & (col('2014')<col('2013')) & (col('2014')<col('2015')),'2014').
   when((col('2015') < col('2006')) & (col('2015')<col('2007')) & (col('2015')<col('2008')) & (col('2015')<col('2009')) & (col('2015')<col('2010')) & (col('2015')<col('2011')) & (col('2015')<col('2012')) & (col('2015')<col('2013')) & (col('2015')<col('2014')),'2015').
   otherwise('more than one min')).show()
# isws iparxei kaliteros tropos!