import os
import sys

# Use the current Python executable for Spark workers
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Matrix Operations") \
    .master("local[1]") \
    .getOrCreate()

sc = spark.sparkContext

A = [
    [1, 2],
    [3, 4]
]

B = [
    [5, 6],
    [7, 8]
]

rddA = sc.parallelize(A)
rddB = sc.parallelize(B)

print("Matrix A:")
for row in A:
    print(row)

print("\nMatrix B:")
for row in B:
    print(row)

# Matrix Addition
addition = rddA.zip(rddB).map(
    lambda rows: [x + y for x, y in zip(rows[0], rows[1])]
)

print("\nMatrix Addition:")
for row in addition.collect():
    print(row)

# Matrix Subtraction
subtraction = rddA.zip(rddB).map(
    lambda rows: [x - y for x, y in zip(rows[0], rows[1])]
)

print("\nMatrix Subtraction:")
for row in subtraction.collect():
    print(row)

# Matrix Multiplication
B_transpose = list(zip(*B))

multiplication = rddA.map(
    lambda row: [
        sum(x * y for x, y in zip(row, column))
        for column in B_transpose
    ]
)

print("\nMatrix Multiplication:")
for row in multiplication.collect():
    print(row)

spark.stop()