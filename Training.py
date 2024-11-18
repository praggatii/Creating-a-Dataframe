from pyspark.sql import SparkSession
from pyspark import SparkContext
 
# Initialize Spark Session
spark = SparkSession.builder.appName('Create DataFrame').getOrCreate()
 
 
# Check Spark Session by creating simple DataFrame
data = [[("Alice", 25) , ("Neo", 24) , ("Vatsal", 22)],
        [("Huma", 21) , ("Anu", 20), ("Pri", 23)]]
columns = ["Name" , "Age"]
 
# Create DataFrame from List
df = spark.createDataFrame(data,columns)
df.show()

