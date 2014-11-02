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

def turnToKeyAndValueP(string):
	return string[string.find("\'")+1:string.find(",")-1],float(string[string.find(",")+1:string.rfind(")")])
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
def makeSparseMatrix(daily_vocabulary):
	sparseMatrix={}
	sparseMatrixtemp=[]
	total=0
	returnsparseMatrix=[]
	daily_dict={}
	for key, value in daily_vocabulary:
		daily_dict.setdefault(key, value)
	for voc in ngramPValue:	
		if voc in daily_dict:
			sparseMatrix[voc]=daily_dict[voc]
		else:
			sparseMatrix[voc]=0
	sortedsparseMatrix=sorted(sparseMatrix.items())
	for everyVocCount in sortedsparseMatrix:
		sparseMatrixtemp.append(everyVocCount[1])
	#total=sum(sparseMatrixtemp)
	#for orginal_number in sparseMatrixtemp:
	#	returnsparseMatrix.append(orginal_number*1.0/total)
	return sparseMatrixtemp

conf = SparkConf().setAppName("test").setMaster("local")
sc = SparkContext(conf=conf)
dayandindexstat=getIndexData("ni",0.5)#don't forget to insert indexname
ngramResult = sc.textFile("hdfs:////ngramresult").map(turnToKeyAndValue).collect()#insert the path
ngramPValue = sc.textFile("hdfs:////ngramPresult").map(turnToKeyAndValueP).collectAsMap()#insert the path
newNgramPvalue={}
newNgram=[]
for clearSlash in ngramPValue:
	if "\\" not in clearSlash:
		newNgramPvalue[clearSlash]=ngramPValue[clearSlash]
for ngramRow in ngramResult:
	try:
		if newNgramPvalue[ngramRow[1][0]]>0.05:
			newNgram.append(ngramRow)
	except:
		continue

afterClean=sc.parallelize(newNgram)
aftergroup=sc.parallelize(map((lambda (x,y): (x, list(y))), sorted(afterClean.groupByKey().collect())))
afterChange=aftergroup.map(lambda (x,y):(searchForDayStat(x),makeSparseMatrix(y))).filter(lambda (x,y):x!=None).map(lambda (x,y):LabeledPoint(x,y))
model = NaiveBayes.train(afterChange)
print model
#model.predict()

