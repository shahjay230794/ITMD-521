# ITMD-521 Read & Write Assignment

## Cluster Command
Command used to generate csv file :
```bash
spark-submit --verbose --name jks-read-write-csv-1998.py --master yarn --deploy-mode cluster jks-read-write-csv-1998.py
```
Command used to generate parquet file :
```bash
spark-submit --verbose --name jks-read-write-parquet-1998.py --master yarn --deploy-mode cluster jks-read-write-parquet-1998.py
```

### Deliverable 1 
Referred chapter 6 (working with different types of data) from Ebook for different sql commands in order to extract year from date column from a decade.Also checked Pyspark documentation in order to get error free syntax along with import statements for functions such as IntegerType,to_date,String type,etc. 
I have first analyzed actual data in the data frame.This implies there is a value column in the schema after reading the decade text file.Then i parsed the entire file  using the command reference provided in instructions pdf.This yields the data was displayed in multiple columns on the trigger of dataframe show command.As a result,we understand that there exists a date column and we have to filter data according to one year provided to us from the decade file ie. to extract 1998 from 90.txt in my case.
I have used the filter command on date column , then cast the date type to String and mention 1998 as string input to be filtered.
In other words, the filter command converts date to string , looks for year 1998 and then displays the filtered data as it is in date format along with remaining columns of the schema. 

### Deliverable 2
There exists PERMISSIVE mode while reading a file in order to deal to with bad data.As per chapter 9 from text book, it tells us about the different read modes in Spark.Permissive, dropMalformed and failFast are the three read modes in Spark with permissive mode being the default. As a result, it is clearly evident that the default mode has been applied while reading the schema and it helps to set all field values to nulls when it encounters a corrupted record. I am able to see null records in my logs , hence the schema is handling nulls by the permissive mode. We can also handle bad data like "99999" using fill() or fillna() functions as per pyspark documentation.
Using isNotNull() and isNull() functions return boolean result ie. true or false whether expression is NOT Null or Null respectively.While defining the schema, parameter called nullable which is boolean ie. nullable = True or nullable=False will help us understand whether the field can be null or not.

### Deliverable 3
Post execution, we understand that the input data is large chunk with size equal to around 80GB.As a result, in order to optimize and reduce the execution time we can use compression techniques.This implies that when files are compressed using gzip,bzip2 or lz4 techniques that are generally splittable.Considering the huge input data file, the simplest way to make it splittable is to upload it through separate files.Ideal scenario calls for each file shall be in few megabytes.
There are few direct performance enhancement methods in order to run job in shortest amount of time.
1.Parallelism : The degree of parallelism can be increased when we try to speed up a specific stage.We can configure and set the number of tasks that execute per CPU core in cluster manager.Using spark.default.parallelism and spark.sql.shuffle.partitions we can build the setting as per the number of cores available in cluster.
2.Improved Filtering : Filters can be made use into data sources in order to avoid irrelevant data in respect to the output.Bucketing and enabling partitions can also help to acheive the same.During the initial processing, use of filters help us reduce the execution time.
3.Repartitioning and Coalescing : Repartition method means to create shuffle. We can try to shuffle least amount of data.
Coalescing will try to merge partitions on same node into one partition.The latter, ie. repartition method will shuffle data in order to achieve load balancing through various nodes.There also exists an option to construct custom repartitions at the resilient distributed dataset.This will in a way organize data to better level of precision in order to run the job in shorter span of time.   
