from pyspark import SparkContext, SparkConf

import sys

conf = SparkConf().setAppName("ShapeStrokes").setMaster("local")
sc = SparkContext(conf=conf)

word = sys.argv[1]
dataset1 = sys.argv[2]
dataset2 = sys.argv[3]

shapeRDD = sc.textFile(dataset2)
dict = shapeRDD.collect()

wordlist = shapeRDD.filter(lambda line: word in line.lower())


print(wordlist.collect())
