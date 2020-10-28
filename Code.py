import os
from os import system
system('pip install pyspark')
system('pip install pandas')

from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
df=sqlContext.read.option("header",True).csv("input.csv")
# df.printSchema()
df.show()
df.createOrReplaceTempView('test')
df1=sqlContext.sql("select order_id,order_item,cost,order_dt,from_unixtime(unix_timestamp(cast(order_dt as string),'dd-MMM-yy'),'yyyyDDD') as Julian_Date,expected_disp_dt,order_status from test")
df1.show()
df1.toPandas().to_csv("output.csv",sep=',', header=True, index=False)