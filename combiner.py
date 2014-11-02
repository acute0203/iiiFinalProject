from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("simple").setMaster("local")
sc = SparkContext(conf=conf)
def changeToList(string):
	return((string[2:string.rfind(",")-1],int(string[string.rfind(",")+1:len(string)-1])))
textFile = sc.textFile("hdfs://///RESULT/*/*")#insert the path
afterSorted=textFile.map(changeToList).reduceByKey(lambda total, count: total + count).coalesce(1).sortByKey(True)
afterSorted.saveAsTextFile("hdfs://///RESULT1")#insert the path
