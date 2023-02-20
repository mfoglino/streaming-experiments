import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

logFile = "data.txt"  # Should be some file on your system
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()