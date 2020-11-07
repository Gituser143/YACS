from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StructType, StructField, StringType
import sys

# Load up the SparkContext object
sc = SparkContext()

# Argument parsing
word = sys.argv[1]
dataset1 = sys.argv[2]
dataset2 = sys.argv[3]

# init RDD
shape_statRDD = sc.textFile(dataset2)

# RDD with only required word
wordlistRDD = shape_statRDD.filter(lambda line: word in line.lower())

# Converting RDD to dataframe
sql = SQLContext(sc)
schema = StructType([StructField("word", StringType(), True),
                     StructField("timestamp", StringType(), True),
                     StructField("reccognized", StringType(), True),
                     StructField("key_id", StringType(), True),
                     StructField("Total_Strokes", StringType(), True)])

df = sql.createDataFrame(wordlistRDD.map(lambda s: s.split(",")), schema=schema)

# Calculate avg for Recogonised drawings
x = df.filter(df['reccognized'] == False).agg({"Total_Strokes": "avg"})
# Converts DF object to int
Recognised = (x.collect()[0][0])

# Calculate avg for Not Recogonised drawings
y = df.filter(df['reccognized'] == True).agg({"Total_Strokes": "avg"})
# Converts DF object to int
notRecognised = (y.collect()[0][0])

# If word is not present output 0
# Round answer to 5 digits
if Recognised == None:
    print(0)
else:
    print(round(Recognised,5))

if notRecognised == None:
    print(0)
else:
    print(round(notRecognised,5))