#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# # Main

# In[ ]:


import dfBasics
import common
import encoder
import pfAdapt
#import charts


# In[ ]:


import pandas as pd
from pyspark.sql import functions


# In[ ]:


columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE',       'CSLABILLINGMONTH', 'CSENDERPROTOCOL', 'CSENDERENDPOINTID',       'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT',       'CMESSAGETAT2', 'CSLADELIVERYTIME']
# withot 'CSLABILLINGMONTH'
def get_columns_2():
    columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE',            'CSENDERPROTOCOL', 'CSENDERENDPOINTID',           'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT',           'CMESSAGETAT2', 'CSLADELIVERYTIME']
    return columns

columns = ['CGLOBALMESSAGEID',  'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE', 'CSENDERENDPOINTID', 'CSENDERPROTOCOL', 'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT', 'CMESSAGETAT2', 'CSLADELIVERYTIME']
     

#columns = get_columns_2()
#to count messages sent
#columns = [ 'CSTARTTIME', 'CSENDERENDPOINTID']


# In[ ]:


sparkSession = dfBasics.getSparkSession()


# ## encode columns

# In[ ]:


df = sparkSession.read.parquet("/tmp/sla.parquet")
#senders = pd.read_parquet('/tmp/senders' + '.parquet', engine='pyarrow')


# In[ ]:


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def transform(value):
    return int( _encoder.transform([value])[0])
    
udf_transform = udf(lambda z: transform(z), StringType())

#df2.withColumn("CSENDERENDPOINTID", str( _encoder.transform([df2.CSENDERENDPOINTID])[0])) 
#df2 = df2.withColumn("CSENDERENDPOINTID", udf_transform(df2.CSENDERENDPOINTID)) 


# In[ ]:


senders = sparkSession.read.parquet("/tmp/senders.parquet")
senders = list(senders.toPandas()['CSENDERENDPOINTID'])


# In[ ]:


columns = list(df.limit(1).toPandas().columns)
columns.remove('CGLOBALMESSAGEID')
columns.remove('CSLATAT')
columns.remove('CMESSAGETAT2') 
columns.remove('CSLADELIVERYTIME')
columns.remove('CINBOUNDSIZE')
columns.remove('CSTARTTIME')
columns.remove('CENDTIME')


# In[ ]:


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


# In[ ]:


import numpy as np
import encoder
from pyspark.sql.functions import col

def encode_columns_spark(dataframe=None,columns=None):
    for column in columns:
        global _encoder
        #print (column)
        _encoder = encoder.TolerantLabelEncoder(ignore_unknown=True)
        _encoder.classes_ = np.load('/home/jovyan/work/cls/jupyter/npy/' + column + '.npy')
        #dataall[column] = _encoder.transform(dataall[column]) 
        udf_transform = udf(lambda z: transform(z), StringType())
        dataframe=dataframe.withColumn(column, udf_transform(col(column)).cast("Integer"))
        #df3.head()
    return dataframe


# In[ ]:


def cast_spark_columns(dataframe=None,columns=[],type="int" ):
    for column in columns:
        dataframe = dataframe.withColumn(column, col(column).cast(type))
    return dataframe    


# In[ ]:


def process(sender=None, dataframe=None):
    df3 = dataframe.where(f.col("CSENDERENDPOINTID").isin([sender]))
    df3 = encode_columns_spark(dataframe=df3,columns=columns)
    df3 = df3.withColumn("year", udf_add_year(df3.CSTARTTIME)).withColumn("month", udf_add_month(df3.CSTARTTIME)).withColumn("day", udf_add_day(df3.CSTARTTIME)).withColumn("hour", udf_add_hour(df3.CSTARTTIME)) 
    df3=cast_spark_columns(dataframe=df3, columns=['CSTARTTIME', 'CENDTIME','CINBOUNDSIZE','CSLATAT','CMESSAGETAT2','CSLADELIVERYTIME'], type='long')
    return df3


# In[ ]:


np_load_old = np.load

# modify the default parameters of np.load
np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)

# restore np.load for future normal usage
#np.load = np_load_old


# In[ ]:


#import pyspark.sql.functions as f
#ender = senders[0]
#df4 = process(sender=sender,dataframe=df)
#df4.head()


# In[ ]:


from os import listdir

def listdirectory(path=None,filter='.'):
    return [x for x in listdir(path) if not x.startswith(filter)]    

_files = listdirectory(path='/tmp/enc')
senders = senders[len(_files):]


# In[ ]:


import pyspark.sql.functions as f
#sender = senders[0]
#senders=senders[24:]
for sender in senders:
    df4 = process(sender=sender,dataframe=df)
    df4.write.mode("overwrite").parquet("/tmp/enc/sla_enc_" + sender + ".parquet")

