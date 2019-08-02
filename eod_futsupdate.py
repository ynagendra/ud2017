'''
Created on Dec 10, 2014
Modified on Jul 9, 2016
Redesigned to use pandas on Mar 4, 2017
@author: y n kumar
'''
import io
import csv
import requests
import pandas as pd
import StringIO
import urllib2
import requests
from zipfile import ZipFile
from datetime import date
import subprocess
from df02032017 import index_name
from df02032017 import index_symbol
from df02032017 import getXpirydates
#from df02032017 import getFNOsymbols
from df02032017 import getUpdateFile
from df02032017 import getindexfromNSE
from df02032017 import getCSVFile
from df02032017 import idn2s
from df02032017 import slist
import time
import sys


arguments = sys.argv[1:]
count = len(arguments)
manual=False
if ((count>0) and (arguments[0][0]=='m')):
    manual=True
badfuts=['NIFTYCPSE','NIFTYINFRA','NIFTYMID50','NIFTYPSE','FTSE100','INDIAVIX']
hdr = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'gzip, deflate, br',
       'Accept-Language': 'en-US,en;q=0.8',
       'Referer': 'https://nseindia.com/products/content/equities/equities/homepage_eq.htm',
       'Cookie': '_ga=GA1.2.1945572098.1514393045; NSE-TEST-1=1809850378.20480.0000',
       'Connection': 'keep-alive'}

def getFNOsymbols():
    url = 'https://www.nseindia.com/content/fo/fo_mktlots.csv'
    with requests.Session() as s:
        download = s.get(url)
    
        decoded_content = download.content.decode('utf-8')
    
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        lol = list(cr)
    symbols=[]
    for line in lol:
        symbols.append(line[1])
    for i in range(len(symbols)):
        symbols[i]=symbols[i].strip()
    symbols=sorted(symbols)
    symbols.remove('SYMBOL')
    symbols.remove('Symbol')
    return symbols  
"""
.......get the list of fno symbols 
"""
fnosymbols=getFNOsymbols()

"""
........set the date for which data needs to be obtained from NSE website 
"""
delay = 3
mname=["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"]


if manual:
    s=raw_input('Enter date (dd-mm-yyyy) : ')
    d=date(int(s.split('-')[2]),int(s.split('-')[1]),int(s.split('-')[0]))
    y = d.strftime('%Y')
    dd = d.strftime('%d')
    m = d.strftime('%b').upper()  
    mm = d.strftime('%m')
    cf=y+mm+dd 
else:
    y=str(date.today().year)
    mm=str(date.today().month).zfill(2)
    m = date.today().strftime('%b').upper()
    dd=str(date.today().day).zfill(2)
    cf=y+mm+dd

"""
------------extract fno data
"""
url9="https://www.nseindia.com/content/historical/DERIVATIVES/"+y+"/"+m+"/fo"+dd+m+y+"bhav.csv.zip"
req1 = urllib2.Request(url9, headers=hdr)
zf1 = ZipFile(StringIO.StringIO(urllib2.urlopen(req1).read()))
fnod = pd.read_csv(zf1.open(zf1.namelist()[0]))
fnod.loc[:,'time']='000000'
fnod.loc[:,'per']='D'
fnod.loc[:,'date']=(pd.to_datetime(fnod['TIMESTAMP'],format='%d-%b-%Y')).dt.strftime('%Y%m%d')
xds=list(fnod[(fnod.SYMBOL=='NIFTY') & (fnod.INSTRUMENT=='FUTIDX')].EXPIRY_DT)

"""
------------extract futures(near and mid) data
"""
nfut=fnod[fnod.INSTRUMENT.isin(['FUTIDX','FUTSTK']) & (~fnod.SYMBOL.isin(badfuts)) & (fnod.EXPIRY_DT==xds[0])][['SYMBOL','per','date','time','OPEN','HIGH','LOW','CLOSE','CONTRACTS','OPEN_INT']]
nfut.columns=['<TICKER>','<PER>','<DTYYYYMMDD>','<TIME>','<OPEN>','<HIGH>','<LOW>','<CLOSE>','<VOL>','<OPENINT>']
fp7='e:/chd/gdata1/nfut_'+cf+'.txt'
nfut.to_csv(fp7,index=False)


mfut=fnod[fnod.INSTRUMENT.isin(['FUTIDX','FUTSTK']) & (fnod.EXPIRY_DT==xds[1])][['SYMBOL','per','date','time','OPEN','HIGH','LOW','CLOSE','CONTRACTS','OPEN_INT']]
mfut.columns=['<TICKER>','<PER>','<DTYYYYMMDD>','<TIME>','<OPEN>','<HIGH>','<LOW>','<CLOSE>','<VOL>','<OPENINT>']
fp8='e:/chd/gdata1/mfut_'+cf+'.txt'
mfut.to_csv(fp8,index=False)

cx1=r'C:\Program Files (x86)\Convert2MS\Convert2MS.exe'
px1=r'-e:\chd\gdata\eod\nfuts.cfg'
px2=r'-e:/chd/gdata1/nfut_'+cf+'.txt'
s14="%s %s %s" % (cx1,px1,px2)
rno=subprocess.call (s14)
#time.sleep(delay)
print rno,"near month Futures transferred to database"

cx1=r'C:\Program Files (x86)\Convert2MS\Convert2MS.exe'
px1=r'-e:\chd\gdata\eod\mfuts.cfg'
px2=r'-e:/chd/gdata1/mfut_'+cf+'.txt'
s15="%s %s %s" % (cx1,px1,px2)
rno=subprocess.call (s15)
#time.sleep(delay)
print rno,"mid month Futures transferred to database"
