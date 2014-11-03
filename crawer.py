#!/usr/bin/python
#-*- coding:utf-8 -*-
from sgmllib import SGMLParser
import urllib,time,shutil,re
class handleFuturePrice(SGMLParser):
    def reset(self):
        SGMLParser.reset(self)
        self.number=0
        self.rawString=""
    def start_tr(self,attrs):
        if attrs==[('class', 'row1')] or attrs==[('class', 'row2')]:
            self.number=1
    def handle_data(self, data):
        if self.number==1:
            self.rawString+=data
    def end_tr(self):
        self.number=0
def getFuturePrice(url,parser):
    try:
        URLprice = urllib.urlopen(url)
        parser.feed(URLprice.read())
        URLprice.close()
    except:
        localtime = time.asctime( time.localtime(time.time()) )
        err=open('/home/dsakryk/ETL/DATA/errorlog.txt','a')
        err.writelines(localtime+'\nConnect Error!\n\n')
        err.close()
        return
def checkForDuplicate():
    for FileNumber in range (1,259):
        checked=[]
        file_dir="/home/dsakryk/ETL/DATA/%d.csv"%(FileNumber)
        f=open(file_dir,'r')
        dataReadlines=f.readlines()
        if dataReadlines[len(dataReadlines)-1]!=dataReadlines[len(dataReadlines)-2] or len(dataReadlines)==1:
            f.close()
            continue
        else:
            for everyLineBeforeDuplicate in range(0,len(dataReadlines)-1):
                checked.append(dataReadlines[everyLineBeforeDuplicate])
            f.close()
            f=open(file_dir,'w+')
            for writein in checked:
            	f.writelines(writein)
            f.close()
def checkForForm():
    for FileNumber in range(1,259):
        dataSepWithComma=[]
        dataReadlines=[]
        dataChangeToString=''
        decideSixOrSeven=6
        file_dir="/home/dsakryk/ETL/DATA/%d.csv"%(FileNumber)
        f=open(file_dir, 'r')   
        dataReadlines=f.readlines()
        if len(dataReadlines)>2:
            for item in dataReadlines:
                dataChangeToString+=item+','
            dataSepWithComma=dataChangeToString.split(',')
            if('ML' in dataSepWithComma[0] or 'BC' in dataSepWithComma[0] or dataSepWithComma[0].replace('\xef\xbb\xbf','') in jpdef):
                decideSixOrSeven=7
            if (dataSepWithComma[len(dataSepWithComma)-decideSixOrSeven]==dataSepWithComma[len(dataSepWithComma)-decideSixOrSeven*2]):
                f.close()
                f=open(file_dir, 'w')
                for normalDataIndex in range(len(dataReadlines)-1):
                    f.writelines(dataReadlines[normalDataIndex])  
            if find(dataSepWithComma[len(dataSepWithComma)-3]):
                err=open('/home/dsakryk/ETL/DATA/errorlog.txt','a')
                err.writelines('error message from block:2\n')
                err.writelines(time.asctime(time.localtime(time.time()))+'\n'+file_dir+'\nError Data:\n')
                err.writelines(dataReadlines[len(dataReadlines)-1])
                err.writelines('\n\n')
                err.close()
                f.close()
                f=open(file_dir, 'w')
                for normalDataIndex in range(len(dataReadlines)-1):
                    f.writelines(dataReadlines[normalDataIndex])
                break          
            for compareWithIndexOfZero in range(decideSixOrSeven,len(dataSepWithComma)-1,decideSixOrSeven):
                if dataSepWithComma[0].replace('\xef\xbb\xbf','')!=dataSepWithComma[compareWithIndexOfZero].replace('\xef\xbb\xbf',''):
                    err=open('/home/dsakryk/ETL/DATA/errorlog.txt','a')
                    err.writelines('error message from block:1\n')
                    err.writelines(time.asctime(time.localtime(time.time()))+'\n'+file_dir+'\nError Data:\n')
                    err.writelines(dataReadlines[len(dataReadlines)-1])
                    err.writelines('\n\n')
                    err.close()
                    f.close()
                    f=open(file_dir, 'w')
                    for normalDataIndex in range(len(dataReadlines)-1):
                        f.writelines(dataReadlines[normalDataIndex])
                    break
        f.close()
def find(string):
    if string=='close':
        return False
    match = re.search(r'[0-1]\d:[0-5]\d|2[0-3]:[0-5]\d',string) or re.search(r'(0?[1-9]|1[012])[/](0?[1-9]|[12][0-9]|3[01])',string)
    if match:
        return False
    else:
        return True
def WriteInRawFile(deleteNull):
    count=0
    file_name=1
    for i in range(len(deleteNull)):
        f_dir="/home/dsakryk/ETL/DATA/%d.csv"%(file_name)
        f = open(f_dir,"a")
        if deleteNull[i]=='0.00%' and (is_chinese(deleteNull[i-2]) or is_alphabet(deleteNull[i-2])) and not('ML' in deleteNull[i-2] or 'BC' in deleteNull[i-2] or deleteNull[i-2] in (jpdef or spec)):
            deleteNull.insert(i,'0')
        f.writelines(deleteNull[i])
        f.writelines(',')
        count+=1
        if count==5 and ('ML' in deleteNull[i-4] or 'BC' in deleteNull[i-4] or deleteNull[i-4] in jpdef):
            continue
        else:
            if count>=5:
                f.writelines('\n')
                f.close()
                file_name+=1
                count=0
def calculateExecTime(start,end):
    elapsed = end - start
    f=open('/home/dsakryk/ETL/DATA/Time.txt','a')
    f.write(str(elapsed)+'\n')
    f.close()
def mainFunction():
    startURL = "http://www.stockq.org/"
    parser = handleFuturePrice()
    getFuturePrice(startURL,parser)
    replaceChangeLineWithComma=parser.rawString.replace('\n',',')
    splitByComma=[]
    deleteNull=[]
    splitByComma=replaceChangeLineWithComma.split(',')
    for item in range(len(splitByComma)):
        if splitByComma[item]!='':
            deleteNull.append(splitByComma[item])
    return deleteNull
def is_chinese(uchar):
    if uchar.decode('utf-8') >= u'\u4e00' and uchar.decode('utf-8')<=u'\u9fa5':
        return True
    else:
        return False
def is_alphabet(uchar):
        if (uchar.decode('utf-8') >= u'\u0041' and uchar.decode('utf-8')<=u'\u005a') or (uchar.decode('utf-8') >= u'\u0061' and uchar.decode('utf-8')<=u'\u007a'):
                return True
        else:
                return False
#主程式開始
jpdef=['JP全球政府債','JP新興政府債','JP歐盟政府債','JP英國政府債','JP法國政府債','JP德國政府債']
spec=['30年債殖利率','10年期票利率','5年期票利率','3月期票利率']
start = time.time()
WriteInRawFile(mainFunction())
checkForDuplicate()
checkForForm()
end = time.time()
calculateExecTime(start,end)

