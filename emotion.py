from pyspark import SparkContext, SparkConf
import pypyodbc,numpy
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
def getIndexData(indexName,rate):
	riseOrDown=[]
	afterChange=[]
	theData=[]
	try:
		connect = pypyodbc.connect('driver=freetds;server=;port=1433;uid=;pwd=;database=record_database_daily;timeout=2')
		cur = connect.cursor()
		for inf in cur.execute("select riseOrDown,dateT from %s"%(indexName)):
			theData.append(inf)      
	except pypyodbc.Error:
		print ("Database link error")
	finally:
		cur.close()
		connect.close()
	for everyrow in theData:
		riseOrDown.append(everyrow[0])
	stderror=numpy.std(riseOrDown)*rate
	for everyrow in theData:
		if abs(everyrow[0])>=stderror:
			if everyrow[0]>0:
				afterChange.append((everyrow[1],0))#up
			else:
				afterChange.append((everyrow[1],2))#down
		else:
			afterChange.append((everyrow[1],1))#unchange
	return afterChange

def turnToKeyAndValue(string):
	return (string[string.rfind("(")+1:string.find(",")],(string[string.find(",")+1:string.find(")")],int(string[string.rfind(",")+1:string.rfind(")")])))
def getNewsDate(address):
	return address[address.rfind("/")+1:address.rfind(".")]
def dayAfterNewsstat(theDate):
    	for IndexStat in range(len(dayandindexstat)-1):        
        	if dayandindexstat[IndexStat][0]==theDate:
            		return dayandindexstat[IndexStat+1][1]
def searchForDayStat(date):
	for dayIndex in range(len(dayandindexstat)):
		if(date<=dayandindexstat[dayIndex][0]):
			if (date==dayandindexstat[dayIndex][0]):
				return dayAfterNewsstat(dayandindexstat[dayIndex][0])		
			else:
				return dayAfterNewsstat(dayandindexstat[dayIndex-1][0])
def emotionScore(dayVoc):
	positive=0.0
	negative=0.0
	emotion_dict={}#insert the emotion dictionary
	for everyVoc in dayVoc:
		try:
			if emotion_dict[everyVoc[0]]>0:
				positive+=emotion_dict[everyVoc[0]]*everyVoc[1]
			elif emotion_dict[everyVoc[0]]<0:
				negative+=emotion_dict[everyVoc[0]]*everyVoc[1]
		except:
			continue
	totalScore=positive-negative
	if totalScore ==0:
		totalScore=1
	return [positive/totalScore,abs(negative/totalScore)]
def defineRange(score):
	return [int(score[0]*100),int(score[1]*100)]
conf = SparkConf().setAppName("test").setMaster("local")
sc = SparkContext(conf=conf)
dayandindexstat=getIndexData("ni",0.5)#don't forget to insert indexname
ngramResult = sc.textFile("hdfs:////1ngramresult").map(turnToKeyAndValue)#insert the path
aftergroup=sc.parallelize(map((lambda (x,y): (x, list(y))), sorted(ngramResult.groupByKey().collect())))
afterChange=aftergroup.map(lambda (x,y):(searchForDayStat(x),emotionScore(y))).map(lambda (x,y):[x,defineRange(y)]).filter(lambda (x,y):x!=None).map(lambda (x,y):LabeledPoint(x,y))
#predictdata=aftergroup.map(lambda (x,y):(searchForDayStat(x),emotionScore(y))).map(lambda (x,y):[x,defineRange(y)]).filter(lambda (x,y):x==None).collect()
model = NaiveBayes.train(afterChange)
print model.predict([90, 10])

