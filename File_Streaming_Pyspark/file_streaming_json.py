from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    print("Streaming Application Started ...")

    spark = SparkSession\
            .builder\
            .appName("File Streaming Application - JSON")\
            .master("local[*]")\
            .getOrCreate()

    spark.conf.set("dfs.client.read.shortcircuit.skip.checksum", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    spark.sparkContext.setLogLevel("ERROR")

    # registration_dttm,id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments
    input_json_schema = StructType([
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

    stream_df = spark\
                .readStream\
                .format("json") \
                .schema(input_json_schema) \
                .option("path","input_data/json")\
                .load()

    print(stream_df.isStreaming)
    print(stream_df.printSchema())

    stream_count_df = stream_df \
        .groupBy("country") \
        .count() \
        .orderBy("count", ascending=False) \
        .limit(10)

    write_stream_query = stream_count_df\
        .writeStream\
        .outputMode("complete")\
        .format("console")\
        .option("checkpointLocation", "streaming-checkpoint-loc-json")\
        .trigger(processingTime="10 second")\
        .start()

    write_stream_query.awaitTermination()

    print("Streaming Application Completed.")
