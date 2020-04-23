# In Python Page 228 of E-book
from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.functions import year
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("Demo Spark Python Cluster Program").getOrCreate()
 
#df = spark.read.format("csv").option("inferSchema","true").option("header","true").load("hdfs://namenode/user/controller/ncdc-parsed-csv/20/part-r-00000")
df = spark.read.text("hdfs://namenode/user/controller/ncdc-orig/90.txt")
df2 = df.withColumn('Weather Station', df['value'].substr(5, 6)) \
.withColumn('WBAN', df['value'].substr(11, 5)) \
.withColumn('Observation Date',to_date(df['value'].substr(16,8), 'yyyyMMdd')) \
.withColumn('Observation Hour', df['value'].substr(24, 4).cast(IntegerType())) \
.withColumn('Latitude', df['value'].substr(29, 6).cast('float') / 1000) \
.withColumn('Longitude', df['value'].substr(35, 7).cast('float') / 1000) \
.withColumn('Elevation', df['value'].substr(47, 5).cast(IntegerType())) \
.withColumn('Wind Direction', df['value'].substr(61, 3).cast(IntegerType())) \
.withColumn('WD Quality Code', df['value'].substr(64, 1).cast(IntegerType())) \
.withColumn('Sky Ceiling Height', df['value'].substr(71, 5).cast(IntegerType())) \
.withColumn('SC Quality Code', df['value'].substr(76, 1).cast(IntegerType())) \
.withColumn('Visibility Distance', df['value'].substr(79, 6).cast(IntegerType())) \
.withColumn('VD Quality Code', df['value'].substr(86, 1).cast(IntegerType())) \
.withColumn('Air Temperature', df['value'].substr(88, 5).cast('float') /10) \
.withColumn('AT Quality Code', df['value'].substr(93, 1).cast(IntegerType())) \
.withColumn('Dew Point', df['value'].substr(94, 5).cast('float')) \
.withColumn('DP Quality Code', df['value'].substr(99, 1).cast(IntegerType())) \
.withColumn('Atmospheric Pressure', df['value'].substr(100, 5).cast('float')/ 10) \
.withColumn('AP Quality Code', df['value'].substr(105, 1).cast(IntegerType())) \
.filter(year(to_date(df['value'].substr(16,8), 'yyyyMMdd')).cast(StringType()) == '1998') \
.drop('value')
print(df2.show(10))

# https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame.withColumnRenamed
#dfnew = df.withColumnRenamed(' windDirection', 'windDirection')

dfnew.write.format("parquet").mode("overwrite").save("hdfs://namenode/output/itmd-521/jks/1998-show-10.parquet")
dfnew.write.format("parquet").mode("overwrite").save("hdfs://namenode/output/itmd-521/jks/1998-show-10.csv")