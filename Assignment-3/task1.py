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
dataset1 = sys.argv[2]#not really needed
dataset2 = sys.argv[3]

shapeRDD = sc.textFile(dataset2)
# print(shapeRDD.collect())
# dict = shapeRDD.collect()

wordlistRDD = shapeRDD.filter(lambda line: word in line.lower())


sql = pyspark.SQLContext(sc)
schema = StructType([StructField("word", StringType(), True),
    StructField("timestamp",StringType(),True),
    StructField("reccognized",StringType(),True),
    StructField("key_id",StringType(),True),
    StructField("Total_Strokes",StringType(),True)])

df = sql.createDataFrame(wordlistRDD.map(lambda s: s.split(",")),schema=schema)
print(df.count())
x = df.filter(df['reccognized'] == False).agg({"Total_Strokes": "avg"})
x.show()
y = df.filter(df['reccognized'] == True).agg({"Total_Strokes": "avg"})
y.show()
# word_df = wordlistRDD.toDF()
# word_df.printSchema()
# print(wordlist.collect())


#round answer to 5 digits
#if word is not present output 0
