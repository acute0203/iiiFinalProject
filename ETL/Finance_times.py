# -*- coding: utf-8 -*-  
import requests,re,time,os
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import timedelta

'''
NASDAQ_100 = ('http://search.ft.com/search?q=NASDAQ+100&p=%d')
SP_500 = ('http://search.ft.com/search?q=s%26p+500&p=%d')
Dow_Jones = ('http://search.ft.com/search?q=Dow+Jones+index&p=%d')
FTSE_100 = ('http://search.ft.com/search?q=FTSE+100&p=%d')
Nikkei_225 = ('http://search.ft.com/search?q=Nikkei+225&p=%d')
Hang_Seng = ('http://search.ft.com/search?q=Hang+Seng&p=%d')
DAX_30 = ('http://search.ft.com/search?q=DAX+30&p=%d')
CAC_40 = ('http://search.ft.com/search?q=CAC+40+index&p=%d')
反序時間:
SP_500 = ('http://search.ft.com/search?q=s%26amp%3Bp+500&mq=s%26p+500&t=all&fa=people%2Corganisations%2Cregions%2Csections%2Ctopics%2Ccategory%2Cbrand&s=%2BinitialPublishDateTime&curations=ARTICLES%2CBLOGS%2CVIDEOS%2CPODCASTS&highlight=true&p=%d')
Dow_Jones = ('http://search.ft.com/search?q=Dow+Jones+index&t=all&fa=people%2Corganisations%2Cregions%2Csections%2Ctopics%2Ccategory%2Cbrand&s=%2BinitialPublishDateTime&curations&p={0}')
FTSE_100 = ('http://search.ft.com/search?q=FTSE+100&t=all&fa=people%2Corganisations%2Cregions%2Csections%2Ctopics%2Ccategory%2Cbrand&s=%2BinitialPublishDateTime&curations&p={0}') 至2006/9
Nikkei_225 = ('http://search.ft.com/search?q=Nikkei+225&p=%d')  至20080418
Hang_Seng = ('http://search.ft.com/search?q=Hang+Seng&p=%d')  至20101123
'''

def data_insert(dates,title,summary):
    if not os.path.isfile("d:/news/%s.txt"%(dates)):
        flown = (r'%s.txt'%(dates))  
        newfile= os.path.join('d:/news', flown)
        f = open(newfile,'w')
        f.write(title + summary)
        f.close()
    else:
        f = open("d:/news/%s.txt"%(dates),'a')
        f.write(title + summary)
        f.close()

timenow = datetime.now()

link = "http://search.ft.com/search?q=Hang+Seng&t=all&fa=people%2Corganisations%2Cregions%2Csections%2Ctopics%2Ccategory%2Cbrand&s=%2BinitialPublishDateTime&curations&p={0}"
for links in range(1,401):
    time.sleep(3)
    print "第%d頁==============================================="%(links)
    res = requests.get(link.format(links))
    soup = BeautifulSoup(res.text)
    for row in soup.findAll('li', {'class':'result'}):
        titles = row.find('h3')
        if titles == None :
            break
        summarys = row.find('div',{'class':'field typography'})
        date = row.find('p', {'class':'value'}).text
        title = titles.text.encode('utf-8').strip()
        summary = '\n'+ summarys.text.encode('utf-8').lstrip() + '\n'
        print title
        #print summary
        a = ''.join(date)
        b = datetime.strptime(a, "%B %d, %Y")
        dates = b.strftime("%Y%m%d")
        print dates 
        data_insert(dates,title,summary) 

        
