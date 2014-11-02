#!/usr/bin/env python
#coding=utf-8
'''
ngramList[1][0]<-up <-[0]yes[1]no
ngramList[1][1]<-unchange <-[0]yes[1]no
ngramList[1][2]<-down <-[0]yes[1]no
'''
from pyspark import SparkContext, SparkConf
from operator import add
import pypyodbc,numpy
import scipy.stats as stats
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
def choose_vocabulary(data):
	for assignIndex in range(len(data)):
		for innerIndex in range(len(data[assignIndex])):
			if data[assignIndex][innerIndex]==0:
				data[assignIndex][innerIndex]+=1
	chi2, p, dof, ex =stats.chi2_contingency(data)
	return p
def getNewsDate(address):
	return address[address.rfind("/")+1:address.rfind(".")]
def dayAfterNewsstat(theDate):
    	for IndexStat in range(len(dayandindexstat)-1):        
        	if dayandindexstat[IndexStat][0]==theDate:
            		return dayandindexstat[IndexStat+1][1]
def totalDayCount():
	UP=0
	DOWN=0
	UNCHANGE=0
	for everyDayNews in textNews:
		for dayIndex in range(len(dayandindexstat)):
			if(everyDayNews[0]<=dayandindexstat[dayIndex][0]):
				if (everyDayNews[0]==dayandindexstat[dayIndex][0]):
					if dayAfterNewsstat(dayandindexstat[dayIndex][0])=="UP":
						UP+=1
						break
					elif dayAfterNewsstat(dayandindexstat[dayIndex][0])=="DOWN":
						DOWN+=1
						break
					elif dayAfterNewsstat(dayandindexstat[dayIndex][0])=="UNCHANGE":
						UNCHANGE+=1
						break				
				else:
					if dayAfterNewsstat(dayandindexstat[dayIndex-1][0])=="UP":
						UP+=1
						break
					elif dayAfterNewsstat(dayandindexstat[dayIndex-1][0])=="DOWN":
						DOWN+=1
						break
					elif dayAfterNewsstat(dayandindexstat[dayIndex-1][0])=="UNCHANGE":
						UNCHANGE+=1
						break
	return (UP,UNCHANGE,DOWN)

#-----------main------------
conf = SparkConf().setAppName("test").setMaster("local")
sc = SparkContext(conf=conf)
dayandindexstat=getIndexData("ni",0.5)#don't forget to insert indexname
textNews = sc.wholeTextFiles("hdfs://news/*").map(lambda (address,summary):(getNewsDate(address),summary)).collect()#insert the path
totalDayUp,totalDayUnchange,totalDaydown=totalDayCount()

print("---------------initialing NGram Data------------------------------------")
textNgram = sc.textFile("hdfs:////ngramresult")#insert the path
ngramList=sc.parallelize(sorted(textNgram.map(nGramChangeToList).map(lambda(x):(x,0)).groupByKey().map(lambda(x,y):[x,[[0,0],[0,0],[0,0]]]).collect()))
ngramList.map(trueOrFalseInNews).map(lambda(x,y):(x.encode('utf-8').strip(),choose_vocabulary(y))).coalesce(1).saveAsTextFile("hdfs:////ngramPresult")#insert the path


