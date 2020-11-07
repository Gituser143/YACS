from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark import SQLContext
import sys

# load up the SparkContext object
sc = SparkContext()

# argument parsing
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

# Required answers in dataframe
x = df.filter(df['reccognized'] == False).agg({"Total_Strokes": "avg"})
x.show()
y = df.filter(df['reccognized'] == True).agg({"Total_Strokes": "avg"})
y.show()


# round answer to 5 digits
# if word is not present output 0
