import sys

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

df1 = sc.read.csv(dataset1, inferSchema=True, header=True)
df2 = sc.read.csv(dataset2, inferSchema=True, header=True)

# filter for only the word that was given in input
only_word1 = df1.filter(df1.word == word)
only_word2 = df2.filter(df2.word == word)

inner_join = only_word1.join(only_word2, only_word1.key_id == only_word2.key_id)

not_recognized = inner_join.filter(inner_join.recognized == False)

less_than_k_strokes = not_recognized.filter(not_recognized.Total_Strokes < k)

# counts the number of shapes for each countrycode and sort on countrycode
final = less_than_k_strokes.groupBy(less_than_k_strokes.countrycode).count().sort('countrycode')

# Get data and number of rows
n = final.count()
rows = final.collect()

# Print data
for i in range(0, n):
    country = rows[i][0]
    n = rows[i][1]
    print(country, n, sep=',')
