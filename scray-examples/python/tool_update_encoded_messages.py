#!/usr/bin/env python
# coding: utf-8

# # tool_update_encoded_messages

# ## init

# In[1]:


import dfBasics
import common
import encoder
#import pfAdapt
#import charts


# In[2]:


import pandas as pd
from pyspark.sql import functions


# In[3]:


sparkSession = dfBasics.getSparkSession()
df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/user/admin/sla/v00001/v00002/encoded/all/*/*').dropDuplicates() 
df_org  = sparkSession.read.parquet('hdfs://172.30.17.145:8020/sla_sql_data/*/*').dropDuplicates() 


# ## load encoders

# In[4]:


import dfBasics
import pandas as pd

version_sla = 'v00002'
version     = version_sla + '/v00000'

home_directory  =  '/home/jovyan/work/'
share_directory =  '/home/jovyan/work/share/'
#share_directory =  '/home/jovyan/share/'


# ### init encoders (run only once)

# In[5]:


# - run this paragraph only once !
# - if you get an 'allow_pickle' error you need Kernel/restart kernel

import encoder
import numpy as np

np_load_old = np.load

# modify the default parameters of np.load
np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)

# restore np.load for future normal usage
#np.load = np_load_old


# In[6]:


def get_encoders(columns):
    npy= share_directory + 'sla/' + version_sla + '/npy'
    encoders = {}
    for column in columns:
        _encoder = encoder.TolerantLabelEncoder(ignore_unknown=True)
        #_encoder = TolerantLabelEncoder(ignore_unknown=True)
        _encoder.classes_ = np.load(npy + '/' + column + '.npy')
        encoders[column] = _encoder
    return encoders    


# In[7]:


columns = ['CSTATUS', 'CSERVICE', 'CSENDERENDPOINTID', 'CSENDERPROTOCOL','CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID']
encoders = get_encoders(columns)


# ## functions_encode

# In[8]:


import numpy as np
import encoder
from pyspark.sql.functions import col
import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


# In[9]:


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


# In[10]:


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def transform(value):
    if value == None:
        value = 'None'
    result = int( _encoder.transform_version([value])[0])
    return result
    
#udf_transform = udf(lambda z: transform(z), StringType())


# In[11]:


def encode_columns_spark(dataframe=None,columns=None, encoders=None):
    for column in columns:
        global _encoder
        _encoder = encoders[column]
        #print(column,_encoder)
        udf_transform = udf(lambda z: transform(z), StringType())
        dataframe=dataframe.withColumn(column, udf_transform(col(column)).cast("Integer"))
        #print(dataframe.head())
    return dataframe

def cast_spark_columns(dataframe=None,columns=[],type="int" ):
    for column in columns:
        dataframe = dataframe.withColumn(column, col(column).cast(type))
    return dataframe    

def process(dataframe=None, encoders=None, columns=None, sender=None):
    #df3 = dataframe.where(f.col("CSENDERENDPOINTID").isin([sender]))
    df3 = encode_columns_spark(dataframe=dataframe,columns=columns, encoders=encoders)
    df3 = df3.withColumn("year", udf_add_year(df3.CSTARTTIME)).withColumn("month", udf_add_month(df3.CSTARTTIME)).withColumn("day", udf_add_day(df3.CSTARTTIME)).withColumn("hour", udf_add_hour(df3.CSTARTTIME)).withColumn("minute", udf_add_minute(df3.CSTARTTIME)) 
    df3=cast_spark_columns(dataframe=df3, columns=['CSTARTTIME', 'CENDTIME','CINBOUNDSIZE','CSLATAT','CMESSAGETAT2','CSLADELIVERYTIME'], type='long')
    return df3


# In[12]:


#_encoder.transform_version(['None'])
#_encoder.transform([None])


# ## update

# In[13]:


def process_update(year,month,df,df_org,encoders=None, columns=None):
    df2 = df.where(f.col("year").isin([year])).where(f.col("month").isin([month]))
    
    df3 = df2.where((f.col('CSTATUS')==-1) | (f.col('CSERVICE')==-1) | (f.col('CSENDERENDPOINTID')==-1) | (f.col('CSENDERPROTOCOL')==-1)| (f.col('CRECEIVERPROTOCOL')==-1) | (f.col('CRECEIVERENDPOINTID')==-1))
    ids = np.array(df3.select('CGLOBALMESSAGEID').drop_duplicates().collect())     
    ids = [i[0] for i in ids.tolist()]
    #not_ids = np.array(df2.select('CGLOBALMESSAGEID').filter(df2.CGLOBALMESSAGEID.isin(ids) == False).drop_duplicates().collect())  
    #not_ids = [i[0] for i in not_ids.tolist()]
    
    df_update_1 = df_org.where(f.col("CGLOBALMESSAGEID").isin(ids))
    df_update_2 = process(dataframe=df_update_1, encoders=encoders, columns=columns)
    #print(df_update_2.head())
    df_update_2.write.mode('overwrite').parquet('/home/jovyan/work/output/sla_enc_updated_v00002_v00000_' + str(year) + '_' + str(month) + '.parquet')
    
    #df_keep = df2.where(f.col("CGLOBALMESSAGEID").isin(not_ids))
    df_keep = df2.filter(df2.CGLOBALMESSAGEID.isin(ids) == False)
    df_keep.write.mode('overwrite').parquet('/home/jovyan/work/output/sla_enc_keep_v00002_v00000_' + str(year) + '_' + str(month) + '.parquet')


# In[14]:


"""
ear = 2019
month = 10
df2 = df.where(f.col("year").isin([year])).where(f.col("month").isin([month]))
    
df3 = df2.where((f.col('CSTATUS')==-1) | (f.col('CSERVICE')==-1) | (f.col('CSENDERENDPOINTID')==-1) | (f.col('CSENDERPROTOCOL')==-1)| (f.col('CRECEIVERPROTOCOL')==-1) | (f.col('CRECEIVERENDPOINTID')==-1))
ids = np.array(df3.select('CGLOBALMESSAGEID').drop_duplicates().collect())     
ids = [i[0] for i in ids.tolist()]
not_ids = np.array(df2.select('CGLOBALMESSAGEID').filter(df2.CGLOBALMESSAGEID.isin(ids) == False).drop_duplicates().collect())  
not_ids = [i[0] for i in not_ids.tolist()]

df_update_1 = df_org.where(f.col("CGLOBALMESSAGEID").isin(ids))
df_update_2 = process(dataframe=df_update_1, encoders=encoders, columns=columns)
"""


# In[15]:


#_df3 = encode_columns_spark(dataframe=df_update_1,columns=columns, encoders=encoders)
#_df3.head()


# In[16]:


#df_update_2 = process(dataframe=df_update_1, encoders=encoders, columns=columns)
#df_update_2.head()


# In[ ]:


year = 2021
for month in range(6,13):
        process_update(year,month,df,df_org,encoders=encoders, columns=columns)           


# In[ ]:


years = [2022]
for year in years:
    for month in range(1,13):
        process_update(year,month,df,df_org,encoders=encoders, columns=columns)        


# In[ ]:


year = 2023
for month in range(1,7):
        process_update(year,month,df,df_org,encoders=encoders, columns=columns)     


# In[ ]:




