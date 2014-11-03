#-*- coding: utf-8 -*-
import requests,re,time,os
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import timedelta

'''
NASDAQ_100 = ('https://www.google.com/search?q=NASDAQ+100&gl=us&authuser=0&biw=1536&bih=738&tbm=nws&start=0')
SP_500 = ('https://www.google.com/search?q=S%26P+500+index&gl=us&authuser=0&biw=1536&bih=738&tbm=nws&start=0')
Dow_Jones = ('https://www.google.com/search?q=Dow+Jones+Industrial+Average&gl=us&authuser=0&biw=1536&bih=738&tbm=nws&start=0')
FTSE_100 = ('https://www.google.com/search?q=FTSE+100&gl=us&authuser=0&biw=1536&bih=738&tbm=nws&start=0')
Nikkei_225 = ('https://www.google.com/search?q=Nikkei+225&gl=us&authuser=0&biw=1536&bih=738&tbm=nws&start=0')
Hang_Seng = ('https://www.google.com/search?q=Hang+Seng+Index&gl=us&authuser=0&biw=1536&bih=738&tbm=nws&start=0')
DAX_30 = ('https://www.google.com/search?q=DAX+30+index&gl=us&authuser=0&biw=1536&bih=738&tbm=nws&start=0')
CAC_40 = ('https://www.google.com/search?q=CAC+40+index&gl=us&authuser=0&biw=1536&bih=738&tbm=nws&start=0')
'''

link = ('https://www.google.com/search?q=CAC+40+index&gl=us&authuser=0&biw=1536&bih=738&tbm=nws&start=0')
timenow = datetime.now()

def print_text(link):
    page = requests.get(link)
    soup = BeautifulSoup(page.text)
    for row in soup.findAll('li', {'class':'g'}):   
        titles = row.find('h3')
        summarys = row.find('div',{'class':'st'})
        date = row.find('span', {'class':'f'})
        title =  titles.text.encode('utf-8') + '\n'
        #title_test= titles.text.encode('utf-8')
        Summary_test = summarys.text.encode('utf-8') + '\n'
        cleanCom = r'à|ä|í|ç|õ|ô|é|è|ó|ü|ă|â|ç|õ|'
        text_clean = re.sub(cleanCom,'',title)
        Summary = re.sub(cleanCom,'',Summary_test)
        print title
        print Summary
        titless = text_clean.replace(' ','')
        titlesss = titless.decode('utf-8')
        if titlesss >= u'\u4e00' and titlesss <= u'\u9fa5':
            print '中文排除'
            break
        times = date.text
        time = times.title()[-4:]
        if time.isdigit():	
            a = times.split("-")[-1:]
            b = ''.join(a)
            c = datetime.strptime(b, " %b %d, %Y")
            dates = c.strftime("%Y%m%d")
            print dates 
            data_insert(dates,title,Summary)
       
        else :
            time = times.title()[-9:-4]
            if time == "nutes": 
                number = re.search(r"(\d+)", times).group() 
                time = datetime.now() - timedelta(minutes= int(number))
                dates = time.strftime('%Y%m%d')
                print dates
                data_insert(dates,title,Summary)

            elif time == "Hours": 
                number = re.search(r"(\d+)", times).group() 
                time = datetime.now()-timedelta(hours = int(number))
                dates = time.strftime('%Y%m%d')
                print dates
                data_insert(dates,title,Summary)

            else :	
                number = re.search(r"(\d+)", times).group() 
                time = datetime.now() - timedelta(days= int(number)+1)
                dates = time.strftime('%Y%m%d')
                print dates
                data_insert(dates,title,Summary)
       
    get_Next(link)   

def get_Next(link):
    time.sleep(8)
    page = requests.get(link)
    soup = BeautifulSoup(page.text)
    for i in soup.findAll('td' ,{"class":"b"})[1]:
        hrefs = i['href']            
        if hrefs is None:
            print'FinallAllenkyo'
        else :            
            link = 'https://www.google.com'+hrefs
            print link
            print_text(link)        
        

def data_insert(dates,title,Summary):
    if not os.path.isfile("d:/news/%s.txt"%(dates)):
        flown = (r'%s.txt'%(dates))  
        newfile= os.path.join('d:/news', flown)
        f = open(newfile,'w')
        f.write(title + Summary)
        f.close()
    else:
        f = open("d:/news/%s.txt"%(dates),'a')
        f.write('\n'+ title + Summary)
        f.close()


print_text(link)
            


