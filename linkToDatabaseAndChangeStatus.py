#-*- coding:utf-8 -*-
import pypyodbc,numpy

def getIndexData(indexName,rate):
    riseOrDown=[]
    afterChange=[]
    theData=[]
    connect = pypyodbc.connect('driver=freetds;server=;port=1433;uid=;pwd=;DATABASE=record_database_daily')
    cur = connect.cursor()
    try:
        for inf in cur.execute("select riseOrDown,dateT from %s"%(indexName)):#select roi from AEX where dateT='10:26';
            theData.append(inf)      
    except pypyodbc.Error:
        pass
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
def dayAfterNewsstat(theDate,dayAndIndexStat):
    for IndexStat in range(len(dayAndIndexStat)):        
        if dayAndIndexStat[IndexStat][0]==theDate:
            return dayAndIndexStat[IndexStat+1][1]
        
print(dayAfterNewsstat("20140924",getIndexData("ni",0.5)))


