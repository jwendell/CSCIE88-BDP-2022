import sys

from datetime import datetime, timezone
from pyspark.sql import SparkSession

if len(sys.argv) != 2:
    print("Usage: python problem4.py <DATE in format YYYY-MM-DD:HH>")
    sys.exit(1)

t1 = datetime.strptime(sys.argv[1], '%Y-%m-%d:%H')
t1 = t1.replace(tzinfo=timezone.utc)

ss = SparkSession.builder.appName("Problem3").getOrCreate()
ss.conf.set("spark.sql.session.timeZone", "UTC")

# The path below contains the output of problem 3, i.e., parquet files
parquetFile = ss.read.parquet("/hw5/parquet")
parquetFile.explain("cost")
parquetFile.createOrReplaceTempView("events")

events = ss.sql(f"\
SELECT date_format(timestamp, 'yyyy-MM-dd:HH') as hour, url, COUNT(*) as qty \
FROM   events \
WHERE  date_format(timestamp, 'yyyy-MM-dd:HH') = date_format(from_utc_timestamp('{t1.isoformat()}', 'UTC'), 'yyyy-MM-dd:HH') \
GROUP BY date_format(timestamp, 'yyyy-MM-dd:HH'), url \
")

events.write.csv("/hw5/problem4")
