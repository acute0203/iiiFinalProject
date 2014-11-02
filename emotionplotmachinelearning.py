from pyspark import SparkContext, SparkConf
import pypyodbc,numpy
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
import matplotlib.pyplot as plt
import datetime as dt
import matplotlib.dates as mdates

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
				afterChange.append((everyrow[1],1))#up
			else:
				afterChange.append((everyrow[1],3))#down
		else:
			afterChange.append((everyrow[1],2))#unchange
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
def price(x): return '%1.2f'%x
def emotionScore(dayVoc):
	positive=0.0
	negative=0.0
	emotion_dict={}#insert emotion dictionary
	for everyVoc in dayVoc:
		try:
			if emotion_dict[everyVoc[0]]>0:
				positive+=emotion_dict[everyVoc[0]]*everyVoc[1]
			elif emotion_dict[everyVoc[0]]<0:
				negative+=emotion_dict[everyVoc[0]]*everyVoc[1]
		except:
			continue
	#totalScore=positive-negative
	#if totalScore ==0:
	#	totalScore=1
	return [positive,abs(negative)]
def defineRange(score):
	return [int(score[0]),int(score[1])]
def plotgram(afterChange):
	color_dict={1:"red",2:"green",3:"blue"}
	fig = plt.figure()
	fig.suptitle('Relationship of index status and emotion score', fontsize=14, fontweight='bold')
	ax = fig.add_subplot(111)
	fig.subplots_adjust(top=0.85)
	ax.set_xlabel("positive score")
	ax.set_ylabel("negative score")
	for every in afterChange:
		plt.scatter(every[1][0],every[1][1],color=color_dict[every[0]])	
	plt.savefig("scatteremotion.png")#insert local path
def emotionplot(afterChange):
	afterChange=afterChange[-30:]
	fig, ax = plt.subplots(figsize=(15, 6))	
	x = [dt.datetime.strptime(d[0],'%Y%m%d').date() for d in afterChange]
	positive = [price(d[1][0])for d in afterChange]
	negative = [price(d[1][1])for d in afterChange]
	#component= [int(price(d[1][0]))-int(price(d[1][1]))for d in afterChange]
	fig.suptitle("Day and emotion score",fontsize=14, fontweight='bold')
	ax.set_xlabel("Date",fontsize=14, fontweight='bold')
	ax.set_ylabel("Score",fontsize=14, fontweight='bold')
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d'))
	plt.gca().xaxis.set_major_locator(mdates.DayLocator())
	plt.plot(x,negative,label="$Negative score$",color="green",linewidth=2)
	plt.plot(x,positive,label="$Positive score$",color="red",linewidth=2)
	#plt.plot(x,component,label="component(P-N) score$",color="black",linewidth=2)
	plt.gcf().autofmt_xdate()
	plt.savefig("/emotionscore.png")#insert local path
conf = SparkConf().setAppName("test").setMaster("local")
sc = SparkContext(conf=conf)
dayandindexstat=getIndexData("ni",0.1)#don't forget to insert indexname
ngramResult = sc.textFile("hdfs:////1ngramresult").map(turnToKeyAndValue)#insert the path
aftergroup=sc.parallelize(map((lambda (x,y): (x, list(y))), sorted(ngramResult.groupByKey().collect())))
afterChange=aftergroup.map(lambda (x,y):(x,emotionScore(y))).filter(lambda (x,y):x!=None)
#emotionplot(afterChange.collect())
statAndEmotionscore=afterChange.map(lambda (x,y):(searchForDayStat(x),y)).filter(lambda (x,y):x!=None)
#plotgram(statAndEmotionscore.collect())
traindata=statAndEmotionscore.map(lambda (x,y):LabeledPoint(x,y))
predictdata=aftergroup.map(lambda (x,y):(searchForDayStat(x),emotionScore(y))).filter(lambda (x,y):x==None).collect()
model = NaiveBayes.train(traindata)
finalResult=model.predict(predictdata[len(predictdata)-1][1])
filedoc=open("/home/finalResult.txt","w")#insert local path
filedoc.write(str(finalResult))
filedoc.close()

