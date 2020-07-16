#!/usr/bin/env python
# coding: utf-8

# In[1]:


import dfBasics
import common
import encoder
import pfAdapt
#import charts


# In[2]:


import pandas as pd
from pyspark.sql import functions


# # Lib functions

# # Main

# In[3]:


columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE',       'CSLABILLINGMONTH', 'CSENDERPROTOCOL', 'CSENDERENDPOINTID',       'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT',       'CMESSAGETAT2', 'CSLADELIVERYTIME']
# withot 'CSLABILLINGMONTH'
def get_columns_2():
    columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE',            'CSENDERPROTOCOL', 'CSENDERENDPOINTID',           'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT',           'CMESSAGETAT2', 'CSLADELIVERYTIME']
    return columns
#columns = get_columns_2()
#to count messages sent
columns = [ 'CSTARTTIME', 'CSENDERENDPOINTID']


# In[4]:


sparkSession = dfBasics.getSparkSession()


# In[5]:


df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/sla_sql_data/*/*').select(columns).dropDuplicates()  


# In[6]:


pfall = df.toPandas() 


# In[7]:


from datetime import time
import datetime as dt
import calendar
import pytz
de = pytz.timezone('Europe/Berlin')

# long timestamp
def date(x):
    return  dt.datetime.fromtimestamp(float(x) / 1e3, tz=de)


def adddatecolumns(data,pf,column) :
    data['year'] = pf[column].apply(lambda x: date(x).date().year)
    data['month'] = pf[column].apply(lambda x: date(x).date().month)
    data['day'] = pf[column].apply(lambda x: date(x).date().day)
    data['hour'] = pf[column].apply(lambda x: date(x).time().hour)
    data['minute'] = pf[column].apply(lambda x: date(x).time().minute)
    #data['second'] = pf[column].apply(lambda x: x.time().second)
    #data['microsecond'] = pf[column].apply(lambda x: x.time().microsecond)

def converttimestampcolumnn(pf,tsc) :
    pf[tsc] = pf[tsc].apply(lambda x: dt.datetime.fromtimestamp(float(x) / 1e3))


# In[10]:


def astype(pfall,selected,newtype):
    for each in selected:
        pfall[each] = pfall[each].astype(newtype)


# In[11]:


selected = [  'CSENDERENDPOINTID']

astype(pfall,selected,str) 
encoder.encode(pfall,selected)
#astype(pfall,['CSTARTTIME','CENDTIME','CSLATAT','CMESSAGETAT2','CSLADELIVERYTIME','CINBOUNDSIZE'] ,int) 
#del(pfall['CSLABILLINGMONTH'])
#pfall['CGLOBALMESSAGEID'] = pfall['CGLOBALMESSAGEID'].apply(hash)
pfall = pfall.drop_duplicates()


# In[12]:


# convert timestamp to datetime and add column date
#import calendar
##import pytz
#de = pytz.timezone('Europe/Berlin')
adddatecolumns(pfall,pfall,'CSTARTTIME')


# In[13]:


astype(pfall,['CSTARTTIME'] ,int) 


# In[14]:


pfall.to_parquet('/tmp/msgsenders_date.parquet', engine='fastparquet', compression='GZIP')


# In[16]:


get_ipython().system('mv /tmp/msgsenders_date.parquet /tmp/msgsenders_200716.parquet')

