from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys


conf = SparkConf().setAppName("ShapeStrokes").setMaster("local")
sc = SparkContext(conf=conf)

# Load up the SparkSession object
spark = SparkSession.builder.appName("ShapeStrokes").getOrCreate()

# Argument parsing
word = sys.argv[1]
dataset1 = sys.argv[2]
dataset2 = sys.argv[3]

# Selecting the required dataset
csvRDD = sc.textFile(dataset1)
dataset = dataset1
if 'recognized' not in (csvRDD.collect()[1].split(',')):
    dataset = dataset2

#load dataset as csv to df
df = spark.read.csv(dataset, inferSchema=True, header=True)

#filter dataset to contain only metioned word
df = df.filter(df["word"] == word)
# Calculate avg for Recogonised drawings

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
print(round(Recognized, 5))

if notRecognized == None:
    notRecognized = 0
print(round(notRecognized, 5))
