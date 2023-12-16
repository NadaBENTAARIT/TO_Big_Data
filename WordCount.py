
from pyspark import SparkContext, SparkConf
# Create a Spark configuration
conf=SparkConf().setAppName("WordCountApp")
# Create a Spark context
sc=SparkContext(conf=conf)
#Read input text file
textfile =sc.textFile("/user/cloudera/data.txt") 
word_counts =textfile.flatMap (lambda line: line.split(" ")).filter(lambda word: word != "").map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
word_counts_str = word_counts.map(lambda x: (x[0].encode('utf-8'), x[1]))
# Save the result to an output file
word_counts_str.saveAsTextFile("/home/cloudera/SparkResult4")
#Stop the Spark context
sc.stop()
