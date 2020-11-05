from pyspark import SparkContext, SparkConf
import sys
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark import SQLContext
#load up the SparkContext object
conf = SparkConf().setAppName("ShapeStrokes").setMaster("local")
sc = SparkContext(conf=conf)

#argument parsing
word = sys.argv[1]
dataset1 = sys.argv[2]#not really needed
dataset2 = sys.argv[3]
#init RDD
shapeRDD = sc.textFile(dataset2)

#RDD with only required word
wordlistRDD = shapeRDD.filter(lambda line: word in line.lower())

#Converting RDD to dataframe
sql = SQLContext(sc)
schema = StructType([StructField("word", StringType(), True),
    StructField("timestamp",StringType(),True),
    StructField("reccognized",StringType(),True),
    StructField("key_id",StringType(),True),
    StructField("Total_Strokes",StringType(),True)])

df = sql.createDataFrame(wordlistRDD.map(lambda s: s.split(",")),schema=schema)

#Required answers
x = df.filter(df['reccognized'] == False).agg({"Total_Strokes": "avg"})
x.show()# print(format(x[0]))
y = df.filter(df['reccognized'] == True).agg({"Total_Strokes": "avg"})
print(y[0])
y.show()
# word_df = wordlistRDD.toDF()
# word_df.printSchema()
# print(wordlist.collect())


#round answer to 5 digits
#if word is not present output 0
