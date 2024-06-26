from pyspark.sql import SparkSession

def main():
    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("SimpleTest") \
        .getOrCreate()

    # Create an RDD from a list
    data = [1, 2, 3, 4, 5]
    rdd = spark.sparkContext.parallelize(data)

    # Perform a simple transformation and action
    result = rdd.map(lambda x: x * 2).collect()

    print("Result:", result)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
