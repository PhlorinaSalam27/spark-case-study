import os
import sys
import time

# Use the current Python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark Streaming Simulation") \
    .master("local[1]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

sc = spark.sparkContext

print("\n========== SPARK STREAMING SIMULATION ==========")
print("Streaming application started...\n")

# Simulate 5 batches of incoming data
for batch_number in range(1, 6):

    # Simulated incoming data
    data = [
        batch_number * 10 + 1,
        batch_number * 10 + 2,
        batch_number * 10 + 3,
        batch_number * 10 + 4,
        batch_number * 10 + 5
    ]

    # Create an RDD for this batch
    rdd = sc.parallelize(data)

    # Perform Spark processing
    count = rdd.count()
    total = rdd.sum()
    maximum = rdd.max()

    print("----------------------------------------")
    print(f"Batch {batch_number}")
    print("----------------------------------------")
    print("Incoming data:", data)
    print("Number of records:", count)
    print("Total:", total)
    print("Maximum:", maximum)

    time.sleep(2)

print("\n========== STREAMING COMPLETED ==========")

spark.stop()

print("Spark session stopped successfully.")