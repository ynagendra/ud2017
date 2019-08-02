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
from pprint import pprint
from df02032017 import getUpdateFile
from df02032017 import getCSVFile
from df02032017 import idn2s
import time
import sys


arguments = sys.argv[1:]
count = len(arguments)
manual=False
if ((count>0) and (arguments[0][0]=='m')):
    manual=True
    
hdr = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:57.0) Gecko/20100101 Firefox/57.0',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'gzip, deflate, br',
       'Accept-Language': 'en-US,en;q=0.8',
       'Referer': 'https://nseindia.com/products/content/equities/equities/homepage_eq.htm',
       'Cookie': '_ga=GA1.2.1945572098.1514393045; NSE-TEST-1=1809850378.20480.0000',
       'Connection': 'keep-alive'}
badfuts=['NIFTYCPSE','NIFTYINFRA','NIFTYMID50','NIFTYPSE','FTSE100','INDIAVIX']

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
--------get the bhav file for the equities for the day 
"""

csvf = getUpdateFile(m,int(y),int(dd),False)   
df = pd.read_csv(StringIO.StringIO(csvf),header=0)

df=df[df.SERIES=='EQ']
df.loc[:,'time'] = '000000'
df.loc[:,'per'] = 'D'
df.loc[:,'oi'] = 0
df.loc[:,'date']=(pd.to_datetime(df.TIMESTAMP,format='%d-%b-%Y')).dt.strftime('%Y%m%d')
msf = df[['SYMBOL','per','date','time','OPEN','HIGH','LOW','CLOSE','TOTTRDQTY','oi']]
url='https://nseindia.com/corporates/datafiles/LDE_EQUITIES_MORE_THAN_5_YEARS.csv'
s = requests.get(url).content
ds = pd.read_csv(io.StringIO(s.decode('utf-8')))
eqsyms = list(ds.Symbol)
eqsyms.sort()

msf=msf[msf.SYMBOL.isin(eqsyms)]
msf.columns=['<TICKER>','<PER>','<DTYYYYMMDD>','<TIME>','<OPEN>','<HIGH>','<LOW>','<CLOSE>','<VOL>','<OPENINT>']
fp1='e:/chd/gdata1/eq_'+cf+'.txt'
msf.to_csv(fp1,index=False)
"""
----------extract fno underlying data for the day
"""
fnoul=msf[msf['<TICKER>'].isin(fnosymbols)]
fp2='e:/chd/gdata1/ul_'+cf+'.txt'
fnoul.to_csv(fp2,index=False)

"""
-----------extract indices data for the day
"""
url1='https://www.nseindia.com/content/indices/ind_close_all_'+dd+mm+y+'.csv'
s=requests.get(url1).content
idx1=pd.read_csv(io.StringIO(s.decode('utf-8')))
#str1=getCSVFile(url1)
#idx1=pd.read_csv(StringIO.StringIO(str1),header=0)
idx1.loc[:,'time']='000000'
idx1.loc[:,'per']='D'
idx1.loc[:,'oi']=0
idx1.loc[:,'date']=(pd.to_datetime(idx1['Index Date'],format='%d-%m-%Y')).dt.strftime('%Y%m%d')
idx1.loc[:,'symbol']=''
for i in idx1.index:
    try:
        idx1.loc[i,'symbol']=idn2s[idx1.loc[i,'Index Name']]
    except:
        print "unlisted index ",idx1.loc[i,'Index Name']
    print idx1.loc[i,'Index Name'],' --- ',idx1.loc[i,'symbol']
idx=idx1[['symbol','per','date','time','Open Index Value','High Index Value','Low Index Value','Closing Index Value','Volume','oi']]
idx.columns=['<TICKER>','<PER>','<DTYYYYMMDD>','<TIME>','<OPEN>','<HIGH>','<LOW>','<CLOSE>','<VOL>','<OPENINT>']
fp3='e:/chd/gdata1/idx_'+cf+'.txt'
idx.to_csv(fp3,index=False)

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

"""
-------------transfer to metastock database
"""
cx1=r'C:\Program Files (x86)\Convert2MS\Convert2MS.exe'
px1=r'-e:\chd\gdata\eod\eq.cfg'
px2=r'-e:/chd/gdata1/eq_'+cf+'.txt'
s11="%s %s %s" % (cx1,px1,px2)
rno=subprocess.call (s11)
#time.sleep(delay)
print rno,'Equities transferred to database'

cx1=r'C:\Program Files (x86)\Convert2MS\Convert2MS.exe'
px1=r'-e:\chd\gdata\eod\ul.cfg'
px2=r'-e:/chd/gdata1/ul_'+cf+'.txt'
s12="%s %s %s" % (cx1,px1,px2)
rno=subprocess.call (s12)
#time.sleep(delay)
print rno,'FNO Underlyings transferred to database'


cx1=r'C:\Program Files (x86)\Convert2MS\Convert2MS.exe'
px1=r'-e:\chd\gdata\eod\idx.cfg'
px2=r'-e:/chd/gdata1/idx_'+cf+'.txt'
s13="%s %s %s" % (cx1,px1,px2)
rno=subprocess.call (s13)
#time.sleep(delay)
print rno,'Indices transferred to database'

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
