from pyspark.sql import SparkSession

ss = SparkSession.builder.appName("Problem2-Query2").getOrCreate()

# Read avro files
df = ss.read.format("avro").load("/hw5/logs_avro")

# Get the counts
# First, create a map with Hour+URL->User
# Then, group this map by key. This generates a map whose values are a list of Users for each hour+URL key
# Finally we make this list a set, thus eliminating duplicates. The length of the set is the number of unique users per URL per hour
counts = df.rdd\
    .map(lambda fields: (fields[1][0:13] + "-" + fields[2], fields[3]))\
    .groupByKey()\
    .mapValues(lambda values: len(set(values)))


print(counts.toDebugString().decode("utf-8"))

counts.saveAsTextFile("/hw5/problem2-q2")
