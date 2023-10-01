from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Streaming Word Count") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()

    logger = Log4j(spark)

    # Read
    lines_df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "9999") \
        .load()

    # Transform
    words_df = lines_df.select(expr("explode(split(value, ' ')) as word"))
    counts_df = words_df.groupBy("word").count()

    # Sink
    word_count_query = counts_df.writeStream \
            .format("console") \
            .option("checkpointLocation", "chk-point-dir") \
            .outputMode("complete") \
            .start()

    logger.info("Listening to localhost:9999")
    word_count_query.awaitTermination()
