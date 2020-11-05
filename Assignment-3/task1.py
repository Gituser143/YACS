from pyspark import SparkContext, SparkConf
import pyspark
import sys
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
# from pyspark.sql.

conf = SparkConf().setAppName("ShapeStrokes").setMaster("local")
sc = SparkContext(conf=conf)

word = sys.argv[1]
dataset1 = sys.argv[2]
dataset2 = sys.argv[3]

shapeRDD = sc.textFile(dataset2)
# dict = shapeRDD.collect()

wordlistRDD = shapeRDD.filter(lambda line: word in line.lower())
# lst = []
# for line in wordlistRDD.collect():
#     line.split(",")
#     lst.append(line)

# rdd = sc.parallelize(lst)

sql = pyspark.SQLContext(sc)
schema = StructType([StructField("word", StringType(), True),StructField("timestamp",StringType(),True),
StructField("reccognized",StringType(),True),StructField("key_id",IntegerType(),True),
StructField("Total_Strokes",IntegerType(),True)])

df = sql.createDataFrame(data=wordlistRDD,schema=schema)
print(df.schema)
# x = wordlistRDD.filter(wordlistRDD['reccognized'] == True).agg({"Total_Strokes": "avg"})
# print(x)
# word_df = wordlistRDD.toDF()
# word_df.printSchema()
# print(wordlist.collect())
#round answer to 5 digits
#if word is not present output 0
