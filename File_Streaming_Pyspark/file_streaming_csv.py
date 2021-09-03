import findspark

findspark.init('C:/spark3')
from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    print("Application Started ...")

    spark = SparkSession \
        .builder \
        .appName("File Streaming Application - CSV") \
        .master("local[*]") \
        .getOrCreate()
    spark.conf.set("dfs.client.read.shortcircuit.skip.checksum", "true")
    spark.sparkContext.setLogLevel("ERROR")

    # registration_dttm,id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments
    input_csv_schema = StructType([
        StructField("registration_dttm", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("cc", StringType(), True),
        StructField("country", StringType(), True),
        StructField("birthdate", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("title", StringType(), True),
        StructField("comments", StringType(), True)
    ])

    # .schema(input_csv_schema) \

    stream_df = spark \
        .readStream \
        .format("csv") \
        .option("header", "true") \
        .schema(input_csv_schema) \
        .load(path="input_data/csv")

    stream_df.printSchema()

    stream_df_query = stream_df \
        .writeStream \
        .format("console") \
        .start()

    stream_df_query.awaitTermination()

    print("Application Completed.")
