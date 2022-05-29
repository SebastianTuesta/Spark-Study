def split_text(line):
    line = line.split(",")
    return (int(line[0]), float(line[2]))

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
conf.set('spark.logConf', 'true') 
sc = SparkContext(conf = conf)
sc.setLogLevel("OFF")

lines = sc.textFile("customer-orders.csv")
orders = lines.map(split_text)
results = orders.reduceByKey(lambda x, y: x+y).sortBy(lambda x: -x[1])

for result in results.collect():
    print(result)