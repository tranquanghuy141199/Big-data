

a = [1,3,5,7,9]
print(a)

binhphuong = lambda x: x**2
print(binhphuong(3))

print()

pip install pyspark

from pyspark import SparkContext,SparkConf
import collections
conf = SparkConf().setMaster("local").setAppName("max")
sc =SparkContext.getOrCreate(conf=conf)

text = "to be or not to be".split()
number = [12,15,50,90,7,5,17]
rdd = sc.parallelize(number)
# print(rdd)
countsMax = rdd.map(lambda number : ("max",number))
countsAvg = rdd.map(lambda number : ("Avg",number))
print(counts.collect())

"""to be or not to be"""

maxValue = countsMax.reduceByKey(lambda x,y: max(x,y))
def Average(lst): 
    return sum(lst) / len(lst) 
avg = countsAvg.reduceByKey(lambda x,y: Average([x,y]))
print(avg.collect())
print(maxValue.collect())

"""# New Section"""