from pyspark.sql import SparkSession

ss = SparkSession.builder.appName("Problem3").getOrCreate()

# Read avro files
df = ss.read.format("avro").load("/hw5/logs_avro")

# Save using the parquet format
df.write.parquet(path="/hw5/parquet", compression="snappy")
