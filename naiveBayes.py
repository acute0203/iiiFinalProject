from pyspark import SparkContext, SparkConf
import pypyodbc

'''define date and ngram'''
def turnToKeyAndValue(string):
	return (string[string.rfind("(")+1:string.find(",")],(string[string.find(",")+1:string.find(")")],int(string[string.rfind(",")+1:string.rfind(")")])))


'''define ngram and p-value'''
def turnToKeyAndValueP(string):
	return string[1:string.find(",")],float(string[string.find(",")+1:string.rfind(")")])


'''define database'''
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
				afterChange.append((everyrow[1],"UP"))
			else:
				afterChange.append((everyrow[1],"DOWN"))
		else:
			afterChange.append((everyrow[1],"UNCHANGE"))
	return afterChange

def nGramChangeToList(string):
	return ((string[string.find(",")+1:string.rfind(",")-1]))
def trueOrFalseInNews(ngram):
	for everydaynews in textNews:
		if ngram[0] in everydaynews[1]:
			if dayAfterNewsstat(everydaynews[0]) =="UP":
				ngram[1][0][0]+=1
			elif dayAfterNewsstat(everydaynews[0]) =="UNCHANGE":
				ngram[1][1][0]+=1
			elif dayAfterNewsstat(everydaynews[0]) =="DOWN":
				ngram[1][2][0]+=1
	ngram[1][0][1]=totalDayUp-ngram[1][0][0]
	ngram[1][1][1]=totalDayUnchange-ngram[1][1][0]
	ngram[1][2][1]=totalDaydown-ngram[1][2][0]
	return ngram
def dayAfterNewsstat(theDate):
    	for IndexStat in range(len(dayAndIndexStat)):        
        	if dayAndIndexStat[IndexStat][0]==theDate:
            		return dayAndIndexStat[IndexStat+1][1]
def clearKeyWord(data):
	newKeyWords=[]
	for everykeyword in data[1]:
		if keyAndPValue[everykeyword[0]]>0.05:
			newKeyWords.append(everykeyword)
	return (dayAfterNewsstat(data[0]),newKeyWords)

conf = SparkConf().setAppName("test").setMaster("local")
sc = SparkContext(conf=conf)
ngramfiles=sc.textFile("hdfs:////ngramresult")#insert the path
keyAndValue=ngramfiles.map(turnToKeyAndValue)
dateAndstat=map((lambda (x,y): (x, list(y))), sorted(keyAndValue.groupByKey())
