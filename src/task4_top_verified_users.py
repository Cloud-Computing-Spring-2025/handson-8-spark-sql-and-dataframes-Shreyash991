from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, lower

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

# Load datasets
posts_df = spark.read.option("header", True).csv("input/posts.csv", inferSchema=True)
users_df = spark.read.option("header", True).csv("input/users.csv", inferSchema=True)

# TODO: Implement the task here
# Filter for verified users only
print("Users schema:")
users_df.printSchema()

# Convert Verified column to lowercase string and then check for various truth values
verified_users = users_df.filter(
    (lower(col("Verified")) == "true") | 
    (col("Verified") == True) |
    (col("Verified") == "1") |
    (col("Verified") == 1)
)

# Print number of verified users found
print(f"Number of verified users: {verified_users.count()}")

# Join with posts data
verified_posts = posts_df.join(verified_users, "UserID")

# Calculate reach and aggregate by user
user_reach = verified_posts.withColumn("Reach", col("Likes") + col("Retweets")) \
                          .groupBy("UserID", "Username") \
                          .agg(_sum("Reach").alias("Total_Reach")) \
                          .orderBy(col("Total_Reach").desc())

# Print total number of users with reach calculated
print(f"Number of verified users with posts: {user_reach.count()}")

# Get top 5 verified users by reach
top_verified = user_reach.select("Username", "Total_Reach").limit(5)

# Save result
top_verified.coalesce(1).write.mode("overwrite").csv("outputs/top_verified_users.csv", header=True)
