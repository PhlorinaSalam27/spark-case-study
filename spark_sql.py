import os
import sys

# Use the same Python executable for Spark workers
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Spark SQL Programming") \
    .master("local[1]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --------------------------------------------------
# 1. Create student data
# --------------------------------------------------

data = [
    (1, "Shruthi", "CSE", 85),
    (2, "Anu", "CSE", 78),
    (3, "Rahul", "ECE", 92),
    (4, "Priya", "CSE", 88),
    (5, "Arun", "ECE", 70)
]

columns = ["id", "name", "department", "marks"]

df = spark.createDataFrame(data, columns)

print("\n========== STUDENT DATA ==========")
df.show()

# --------------------------------------------------
# 2. Create temporary SQL table
# --------------------------------------------------

df.createOrReplaceTempView("students")

# --------------------------------------------------
# 3. Display all students
# --------------------------------------------------

print("\n========== ALL STUDENTS ==========")

result = spark.sql("""
    SELECT *
    FROM students
""")

result.show()

# --------------------------------------------------
# 4. Students who scored above 80
# --------------------------------------------------

print("\n========== STUDENTS WITH MARKS > 80 ==========")

result = spark.sql("""
    SELECT name, department, marks
    FROM students
    WHERE marks > 80
""")

result.show()

# --------------------------------------------------
# 5. Calculate average marks
# --------------------------------------------------

print("\n========== OVERALL AVERAGE MARKS ==========")

result = spark.sql("""
    SELECT AVG(marks) AS average_marks
    FROM students
""")

result.show()

# --------------------------------------------------
# 6. Average marks by department
# --------------------------------------------------

print("\n========== AVERAGE MARKS BY DEPARTMENT ==========")

result = spark.sql("""
    SELECT department, AVG(marks) AS average_marks
    FROM students
    GROUP BY department
""")

result.show()

# --------------------------------------------------
# 7. Highest scoring student
# --------------------------------------------------

print("\n========== HIGHEST SCORING STUDENT ==========")

result = spark.sql("""
    SELECT name, marks
    FROM students
    ORDER BY marks DESC
    LIMIT 1
""")

result.show()

spark.stop()