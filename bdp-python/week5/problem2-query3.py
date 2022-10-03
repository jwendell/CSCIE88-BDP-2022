from pyspark.sql import SparkSession

ss = SparkSession.builder.appName("Problem2-Query3").getOrCreate()

# Read avro files
df = ss.read.format("avro").load("/hw5/logs_avro")

# Get the counts
# First, create a map with Hour+URL->1
# Then, reduce just sums up the values
counts = df.rdd\
    .map(lambda fields: (fields[1][0:13] + "-" + fields[2], 1))\
    .reduceByKey(lambda a, b: a + b)


print(counts.toDebugString().decode("utf-8"))

counts.saveAsTextFile("/hw5/problem2-q3")
