from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
conf.set('spark.hadoop.fs.s3a.access.key', '*****************')
conf.set('spark.hadoop.fs.s3a.secret.key', '****************************')

spark = SparkSession.builder.appName("Batch").config(conf=conf).getOrCreate()

schema = StructType([StructField('event_time', TimestampType(), True),
                     StructField('event_type', StringType(), True),
                     StructField('product_id', IntegerType(), True),
                     StructField('category_id', IntegerType(), True),
                     StructField('category_code', StringType(), True),
                     StructField('brand', StringType(), True),
                     StructField('price', DoubleType(), True),
                     StructField('user_id', IntegerType(), True),
                     StructField('user_session', StringType(), True), ])

year = datetime.now().year.__str__()
month = datetime.now().month.__str__().zfill(2)
day = datetime.now().day.__str__().zfill(2)
hour = datetime.now().hour.__str__().zfill(2)

df = spark.read.json(f's3a://ecom-user-events/topics/user_activity/year={year}/month={month}/day={day}/hour={hour}/',
                     schema=schema)

data.write.format("jdbc").options(
    url="jdbc:postgresql://postgres:3306/postgres",
    driver="org.postgresql.Driver",
    dbtable="user_events",
    user="postgres",
    password="1221"
).mode("append").save()
