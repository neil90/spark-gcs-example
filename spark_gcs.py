import os
import sys

spark_home = '/usr/local/spark'
sys.path.insert(0, spark_home + "/python")
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.1-src.zip'))
os.environ['PYSPARK_SUBMIT_ARGS'] = """--jars gcs-connector-latest-hadoop2.jar pyspark-shell"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()\
    .setMaster("local[8]")\
    .setAppName("Test")   

sc = SparkContext(conf=conf)

#Setup gcs Hadoop Configurations programmatically
#Require Google Service account
sc._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
sc._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
sc._jsc.hadoopConfiguration().set("fs.gs.project.id", "PROJECT ID HERE")
sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.email", "GSC SERVICE ACCOUNT HERE")
sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.enable", "true")
sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.keyfile", "P12 File HERE")

spark = SparkSession.builder\
    .config(conf=sc.getConf())\
    .getOrCreate()
    
df_testfile = spark.read.format("csv")\
    .option("header", "true")\
    .option("delimiter" ,"\t")\
    .option("inferSchema", "true")\
    .load("gs://test_data_folder/test_tab_file.tsv")
    
df_testfile.show()
