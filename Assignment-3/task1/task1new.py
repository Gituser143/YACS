from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql.types import StructType, StructField, StringType
import sys
from pyspark.sql import SparkSession

# import pyspark.spark

conf = SparkConf().setAppName("ShapeStrokes").setMaster("local")
sc = SparkContext(conf=conf)
# df = spark.read.option()
# Load up the SparkContext object
spark = SparkSession.builder.appName("ShapeStrokes").getOrCreate()
# Argument parsing
word = sys.argv[1]
dataset1 = sys.argv[2]
dataset2 = sys.argv[3]

# init RDD
# shape_statRDD = sc.textFile(dataset2)

# RDD with only required word
# wordlistRDD = shape_statRDD.filter(lambda line: word in line.lower())

# Converting RDD to dataframe
# sql = SQLContext(sc)
# schema = StructType([StructField("word", StringType(), True),
#                      StructField("timestamp", StringType(), True),
#                      StructField("recognized", StringType(), True),
#                      StructField("key_id", StringType(), True),
#                      StructField("Total_Strokes", StringType(), True)])


# df = sql.createDataFrame(wordlistRDD.map(lambda s: s.split(",")), schema=schema)
df = spark.read.option("header",True).csv(dataset2);
# Calculate avg for Recogonised drawings

df = df.filter(df["word"] ==word)

x = df.filter(df['recognized'] == False).agg({"Total_Strokes": "avg"})
# Converts DF object to int
Recognized = (x.collect()[0][0])

# Calculate avg for Not Recogonised drawings
y = df.filter(df['recognized'] == True).agg({"Total_Strokes": "avg"})
# Converts DF object to int
notRecognized = (y.collect()[0][0])

# If word is not present output 0
# Round answer to 5 digits
if Recognized == None:
    Recognized = 0
print(round(Recognized,5))

if notRecognized == None:
    notRecognized = 0
print(round(notRecognized,5))