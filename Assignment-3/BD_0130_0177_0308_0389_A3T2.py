#!/usr/bin/python3
import sys
import time

from pyspark.sql import SparkSession

sc = SparkSession\
    .builder\
    .appName("Assignment3T2")\
    .getOrCreate()

if len(sys.argv) != 5:
    print("Missing arguments")
    sys.exit(1)

word = sys.argv[1]
k = sys.argv[2]         # stroke count
dataset1 = sys.argv[3]
dataset2 = sys.argv[4]

begin = time.time()
df1 = sc.read.csv(dataset1, inferSchema=True, header=True)
df2 = sc.read.csv(dataset2, inferSchema=True, header=True)

# filter for only the word that was given in input
only_word_no_part1 = df1.filter(df1.word == word)
only_word_no_part2 = df2.filter(df2.word == word)

# only_word1 =  only_word_no_part1.coalesce(1)      ## improves the time by around 3 seconds (Time = 7.405009508132935, args = bathtub 50)
# only_word2 =  only_word_no_part2.coalesce(1)

only_word1 = only_word_no_part1  # Time = 10.717103481292725, args = bathtub 50
only_word2 = only_word_no_part2

inner_join = only_word1.join(only_word2, only_word1.key_id == only_word2.key_id)

not_recognized = inner_join.filter(inner_join.recognized == False)

less_than_k_strokes = not_recognized.filter(not_recognized.Total_Strokes < k)

# counts the number of shapes for each countrycode and sort on countrycode
final = less_than_k_strokes.groupBy(less_than_k_strokes.countrycode).count().sort('countrycode')

# Get data and number of rows
n = final.count()

end = time.time()

if n == 0:
    print(0)
    sys.exit(0)

rows = final.collect()

# Print data
for i in range(0, n):
    country = rows[i][0]
    num = rows[i][1]
    print("{c},{num}".format(c=country, num=num))

print("Wall clock time = ", end - begin)
