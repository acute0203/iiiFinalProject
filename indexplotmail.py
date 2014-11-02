import pypyodbc
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt
import matplotlib.dates as mdates
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def getIndexData(indexName,rate):
	riseOrDown=[]
	afterChange=[]
	theData=[]
	try:
		connect = pypyodbc.connect('driver=freetds;server=;port=1433;uid=;pwd=;database=record_database_daily;timeout=2')
		cur = connect.cursor()
		for inf in cur.execute("select dateT,index_price from %s"%(indexName)):
			theData.append(inf)      
	except pypyodbc.Error:
		print ("Database link error")
	finally:
		cur.close()
		connect.close()
	for everyrow in theData:
		riseOrDown.append(everyrow)
	return riseOrDown

def plotstockindex(dayandindexstat):
	fig, ax = plt.subplots(figsize=(15, 6))	
	x = [dt.datetime.strptime(d[0],'%Y%m%d').date() for d in dayandindexstat]
	y = [price(d[1])for d in dayandindexstat]
	fig.suptitle("Index price chart",fontsize=14, fontweight='bold')
	ax.set_xlabel("Date",fontsize=14, fontweight='bold')
	ax.set_ylabel("Price",fontsize=14, fontweight='bold')
	plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y/%m/%d'))
	plt.gca().xaxis.set_major_locator(mdates.DayLocator())
	plt.plot(x,y)
	plt.gcf().autofmt_xdate()
	plt.savefig("result/stockindex.png")

def price(x): return '%1.2f'%x

def mail():
	msg = MIMEMultipart()
	att1 = MIMEText(open('result/scatteremotion.png', 'rb').read(), 'base64', 'utf-8')#insert the file dir
	att1["Content-Type"] = 'application/octet-stream'
	att1["Content-Disposition"] = 'attachment; filename="scatteremotion.png"'
	msg.attach(att1)
	
	att2 = MIMEText(open('result/emotionscore.png', 'rb').read(), 'base64', 'utf-8')#insert the file dir
	att2["Content-Type"] = 'application/octet-stream'
	att2["Content-Disposition"] = 'attachment; filename="emotionscore.png"'
	msg.attach(att2)

	att3 = MIMEText(open('result/stockindex.png', 'rb').read(), 'base64', 'utf-8')#insert the file dir
	att3["Content-Type"] = 'application/octet-stream'
	att3["Content-Disposition"] = 'attachment; filename="stockindex.png"'
	msg.attach(att3)


	msg['to'] = ''#insert the reciever
	msg['from'] = ''#insert the sender
	msg['subject'] = 'Our investment Report'
	text="Hello!Dear User!\n\nIf you wanna see how's the emotion score changed in this month.You can check \"emotionscore.png\"\n\nIf you wanna check the Index chart that you subscribe.You can check\"stockindex.png\" \n\n.OtherWise  the \"scatteremotion.png\" is talking about the condition of how discrete the emotion data and it's classification.\n\nAfter our data training.We suggest that you can %s for your strategy.\n\nWish this suggestion can help you!\n\nThank's for your subscribe and have a nice day!"%(resultChange())#insert the message
	msg.attach(MIMEText(text))

	try:
	    server = smtplib.SMTP()
	    server.connect('smtp.live.com:587')
	    server.ehlo()
	    server.starttls()
	    server.login('','')#insert the sender's mail information
	    server.sendmail(msg['from'], msg['to'],msg.as_string())
	    server.quit()
	    print 'Finished'
	except Exception, e:  
	    print str(e) 
def resultChange():
	filedoc=open("/result/finalResult.txt","r")#insert the path
	status=filedoc.read()
	if status =="1.0":
		return" short a put or long a call "
	elif status =="2.0":
		return" make a Strangle(short put and short call) "
	elif status=="3.0":
		return" short a call or buy a put "
		
dayandindexstat=getIndexData("ni",0.1)#don't forget to insert indexname
dayandindexstat=dayandindexstat[-30:]
plotstockindex(dayandindexstat)
mail()
#resultChange()
