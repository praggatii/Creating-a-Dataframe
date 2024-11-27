# Import Required libraries
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, when, lit

# Creating Saprk Session
spark = SparkSession.builder \
         .appName('Reading CSV') \
         .getOrCreate()

# Read CSV into DataFrame
df = spark.read.csv('/home/priyapragati/Downloads/sample.csv', header=True, inferSchema=True)

# Show the loaded data
df.show()

# *************** PERFORMING OPERATIONS ON DATA **************

# New row to insert
new_row_data = (2011, 'A', 'Agriculture, Forestry and Fishing', 'a_0', 'New Variable', 1000, 'COUNT')
new_row = spark.createDataFrame([new_row_data], df.columns)
df_with_insert = df.union(new_row)

print("DataFrame after Insert:")
df_with_insert.show()

# Update an Existing Row
# For example, updating 'Rolling mean employees' variable's value
df_updated = df_with_insert.withColumn( "value",
    when(col("variable") == "Rolling mean employees", 5000).otherwise(col("value")))

print("DataFrame after Update:")
df_updated.show()

# Delete a particular column 
df_dropped = df.drop("unit") 

# Delete a Row
# For example, deleting the row where variable is 'Total income'
df_final = df_updated.filter(col("variable") != "Total income")

print("DataFrame after Delete:")
df_final.show()

# Stop the Saprk Session
spark.stop()

