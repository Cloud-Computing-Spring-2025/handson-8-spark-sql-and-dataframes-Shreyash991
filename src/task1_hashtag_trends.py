from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, trim

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")


# Split the Hashtags column into individual hashtags
hashtags_df = posts_df.filter(col("Hashtags").isNotNull()) \
                     .select(explode(split(col("Hashtags"), ",")).alias("Hashtag"))

# Clean hashtags (remove any whitespace and keep only valid tags)
hashtags_df = hashtags_df.filter(col("Hashtag").isNotNull() & (col("Hashtag") != "")) \
                         .withColumn("Hashtag", trim(col("Hashtag")))

# Count the frequency of each hashtag
hashtag_counts = hashtags_df.groupBy("Hashtag").count().orderBy(col("count").desc())


# Save result
hashtag_counts.coalesce(1).write.mode("overwrite").csv("outputs/hashtag_trends.csv", header=True)
