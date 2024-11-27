# Import required Libraries
from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext.getOrCreate()

# Create RDD
pp_rdd = sc.parallelize([1,2,3,4,5,6])

# To verify is it store in rdd or not
type(pp_rdd)

# To view Rdd
pp_rdd.collect()

# To view partition wise data
pp_rdd.glom().collect()

# Load Data from csv file into a RDD
rdd = sc.textFile('/home/priyapragati/Downloads/sample.csv')

# Split each line into list of values
header = rdd.first()
data_rdd = rdd.filter(lambda line: line != header).map(lambda line: line.split(","))

# ********** Performing Operation on Data **********

# New_data to insert
new_rdd = [('new_id', 'new_value', 'new_value1')] 

# Create an Rdd for the new Data
new_data_rdd = sc.parallelize(new_rdd)

# Insert new data by combining with exitsing Rdd
data_rdd = data_rdd.union(new_data_rdd)


# UPDATE DATA

#Update Record with specific id
# Update a record
updated_data_rdd = data_rdd.map(lambda x: x if x[0] != "id_to_update" else ("id_to_update", "updated_value1", "updated_value2"))

# Delete a record
final_data_rdd = updated_data_rdd.filter(lambda x: x[0] != "id_to_delete")

# Collect and print the final data
final_data = final_data_rdd.collect()
for record in final_data:
    print(record)
