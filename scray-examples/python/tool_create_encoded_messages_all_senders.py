#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# # Main

# In[1]:


import dfBasics
import common
import encoder
import pfAdapt
#import charts


# In[2]:


import pandas as pd
from pyspark.sql import functions


# In[3]:


columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE',       'CSLABILLINGMONTH', 'CSENDERPROTOCOL', 'CSENDERENDPOINTID',       'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT',       'CMESSAGETAT2', 'CSLADELIVERYTIME']
# withot 'CSLABILLINGMONTH'
def get_columns_2():
    columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE',            'CSENDERPROTOCOL', 'CSENDERENDPOINTID',           'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT',           'CMESSAGETAT2', 'CSLADELIVERYTIME']
    return columns

columns = ['CGLOBALMESSAGEID',  'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE', 'CSENDERENDPOINTID', 'CSENDERPROTOCOL', 'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT', 'CMESSAGETAT2', 'CSLADELIVERYTIME']
     

#columns = get_columns_2()
#to count messages sent
#columns = [ 'CSTARTTIME', 'CSENDERENDPOINTID']


# In[4]:


sparkSession = dfBasics.getSparkSession()


# In[5]:


# ## encode columns

# In[9]:

#df = sparkSession.read.parquet("/tmp/sla.parquet")
df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/sla_sql_data/*/*').select(columns).dropDuplicates() 
#senders = pd.read_parquet('/tmp/senders' + '.parquet', engine='pyarrow')


# In[10]:


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def transform(value):
    return int( _encoder.transform([value])[0])
    
udf_transform = udf(lambda z: transform(z), StringType())

#df2.withColumn("CSENDERENDPOINTID", str( _encoder.transform([df2.CSENDERENDPOINTID])[0])) 
#df2 = df2.withColumn("CSENDERENDPOINTID", udf_transform(df2.CSENDERENDPOINTID)) 


# In[11]:


senders = sparkSession.read.parquet("/tmp/senders.parquet")
senders = list(senders.toPandas()['CSENDERENDPOINTID'])


# In[12]:


def get_columns(df):
    columns = list(df.limit(1).toPandas().columns)
    columns.remove('CGLOBALMESSAGEID')
    columns.remove('CSLATAT')
    columns.remove('CMESSAGETAT2') 
    columns.remove('CSLADELIVERYTIME')
    columns.remove('CINBOUNDSIZE')
    columns.remove('CSTARTTIME')
    columns.remove('CENDTIME')
    return columns


# In[13]:


from datetime import time
import datetime as dt
import calendar
import pytz
de = pytz.timezone('Europe/Berlin')
from pyspark.sql.types import IntegerType

# long timestamp
def date(x):
    return  dt.datetime.fromtimestamp(float(x) / 1e3, tz=de)

udf_add_year = udf(lambda z: date(z).date().year, IntegerType())
udf_add_month = udf(lambda z: date(z).date().month, IntegerType())
udf_add_day = udf(lambda z: date(z).date().day, IntegerType())
udf_add_hour = udf(lambda z: date(z).time().hour, IntegerType())
udf_add_minute = udf(lambda z: date(z).time().minute, IntegerType())
udf_add_minute = udf(lambda z: date(z).time().minute, IntegerType())


# In[14]:


import numpy as np
import encoder
from pyspark.sql.functions import col

def encode_columns_spark(dataframe=None,columns=None, npy='/home/jovyan/work/npy'):
    for column in columns:
        global _encoder
        #print (column)
        #_encoder = encoder.TolerantLabelEncoder(ignore_unknown=True)
        #_encoder.classes_ = np.load(npy + '/' + column + '.npy')
        
        _encoder = encoders[column]
        
        #dataall[column] = _encoder.transform(dataall[column]) 
        udf_transform = udf(lambda z: transform(z), StringType())
        dataframe=dataframe.withColumn(column, udf_transform(col(column)).cast("Integer"))
        #df3.head()
    return dataframe


# In[15]:


def cast_spark_columns(dataframe=None,columns=[],type="int" ):
    for column in columns:
        dataframe = dataframe.withColumn(column, col(column).cast(type))
    return dataframe    


# In[16]:


def process(sender=None, dataframe=None):
    df3 = dataframe.where(f.col("CSENDERENDPOINTID").isin([sender]))
    df3 = encode_columns_spark(dataframe=df3,columns=columns)
    df3 = df3.withColumn("year", udf_add_year(df3.CSTARTTIME)).withColumn("month", udf_add_month(df3.CSTARTTIME)).withColumn("day", udf_add_day(df3.CSTARTTIME)).withColumn("hour", udf_add_hour(df3.CSTARTTIME)).withColumn("minute", udf_add_minute(df3.CSTARTTIME)) 
    df3=cast_spark_columns(dataframe=df3, columns=['CSTARTTIME', 'CENDTIME','CINBOUNDSIZE','CSLATAT','CMESSAGETAT2','CSLADELIVERYTIME'], type='long')
    return df3


# In[17]:


np_load_old = np.load

# modify the default parameters of np.load
np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)

# restore np.load for future normal usage
#np.load = np_load_old


# In[18]:


#import pyspark.sql.functions as f
#ender = senders[0]
#df4 = process(sender=sender,dataframe=df)
#df4.head()
#!mkdir /tmp/enc


# In[19]:




# In[6]:


#!mkdir -p /home/jovyan/work/output/enc


# In[7]:


if None in senders:
    senders.remove(None)


# In[8]:


#None in senders


# In[9]:


from os import listdir

def listdirectory(path=None,filter='.'):
    return [x for x in listdir(path) if not x.startswith(filter)]    

_files = listdirectory(path='/home/jovyan/work/output/enc')
senders = senders[len(_files):]

columns = ['CSTATUS','CSERVICE','CSENDERENDPOINTID','CSENDERPROTOCOL','CRECEIVERPROTOCOL','CRECEIVERENDPOINTID']


# In[11]:


npy='/home/jovyan/work/npy'
encoders = {}
for column in columns:
    _encoder = encoder.TolerantLabelEncoder(ignore_unknown=True)
    _encoder.classes_ = np.load(npy + '/' + column + '.npy')
    encoders[column] = _encoder


# In[12]:


#encoders


# In[13]:


#import pyspark.sql.functions as f
#sender = senders[0]
#print(sender)
#df4 = process(sender=sender,dataframe=df)
#df4.write.mode("overwrite").parquet("/home/jovyan/work/output/enc/sla_enc_" + sender + ".parquet")


# In[ ]:


import pyspark.sql.functions as f
#sender = senders[0]
for sender in senders:
    df4 = process(sender=sender,dataframe=df)
    df4.write.mode("overwrite").parquet("/home/jovyan/work/output/enc/sla_enc_" + sender + ".parquet")


# In[ ]:





# In[ ]:




