from pyspark import SparkContext, SparkConf
from pyspark.sql import functions as F
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

# load dataset as csv to df
df = spark.read.csv(dataset, inferSchema=True, header=True)

# filter dataset to contain only metioned word
df = df.filter(df["word"] == word)

# Calculate avg for Recogonised drawings
avg = df.groupBy(df["recognized"]).agg(F.mean('Total_Strokes'))

x = avg.filter(avg['recognized'] == True)
y = avg.filter(avg['recognized'] == False)

if x.count() == 0:
    recognized = 0
else:
    recognized = (x.collect()[0][-1])

if y.count() == 0:
    notRecognized = 0
else:
    notRecognized = (y.collect()[0][-1])


# If word is not present output 0
# Round answer to 5 digits
if recognized == None:
    recognized = 0
print("%.5f" % recognized)

if notRecognized == None:
    notRecognized = 0
print("%.5f" % notRecognized)
