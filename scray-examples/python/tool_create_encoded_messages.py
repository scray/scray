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
#df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/user/admin/sla/v00001/v00002/encoded/all/*/*').dropDuplicates() 
df  = sparkSession.read.parquet('/home/jovyan/work/output/v00003_v00000.parquet')


# In[ ]:


#df.limit(1).toPandas()


# ## load encoders

# In[ ]:


import dfBasics
import pandas as pd

version_sla = 'v00003'
version     = version_sla + '/v00001'

home_directory  =  '/home/jovyan/work/'
share_directory =  '/home/jovyan/work/share/'
#share_directory =  '/home/jovyan/share/'


# ### init encoders (run only once)

# In[ ]:


# - run this paragraph only once !
# - if you get an 'allow_pickle' error you need Kernel/restart kernel

import encoder
import numpy as np

np_load_old = np.load

# modify the default parameters of np.load
np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)

# restore np.load for future normal usage
#np.load = np_load_old


# In[ ]:


def get_encoders(columns):
    npy= share_directory + 'sla/' + version_sla + '/npy'
    encoders = {}
    for column in columns:
        _encoder = encoder.TolerantLabelEncoder(ignore_unknown=True)
        #_encoder = TolerantLabelEncoder(ignore_unknown=True)
        _encoder.classes_ = np.load(npy + '/' + column + '.npy')
        encoders[column] = _encoder
    return encoders    


# In[ ]:


columns = ['CSTATUS', 'CSERVICE', 'CSENDERENDPOINTID', 'CSENDERPROTOCOL','CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID']
encoders = get_encoders(columns)


# ## functions_encode

# In[ ]:


import numpy as np
import encoder
from pyspark.sql.functions import col
import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


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
udf_add_minute = udf(lambda z: date(z).time().minute, IntegerType())


# In[ ]:


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def transform(value):
    if value == None:
        value = 'None'
    #result = int( _encoder.transform_version([value])[0])
    result = int( _encoder.transform([value])[0])
    return result
 
    
#udf_transform = udf(lambda z: transform(z), StringType())


# In[ ]:


import numpy as np
"""
def transform(value,column=None, npy= share_directory + 'sla/' + version_sla + '/npy'):
    if value == None:
        value = 'None'
    print(column,value)    
    np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)
    _encoder = encoder.TolerantLabelEncoder(ignore_unknown=True)
    _encoder.classes_ = np.load(npy + '/' + column + '.npy')
    return int( _encoder.transform([value])[0])
"""
#udf_transform = udf(lambda z: transform(z), StringType())


# In[ ]:


"""
def check_transformed(value0,column,value):
    if value0 == None or value == None:
        return False
    return value0 == transform(value,column=column)

def calc_error(columns):
    status = columns[0]
    service = columns[1]
    error=int(not( ( check_transformed(status,'CSTATUS','PENDING') & check_transformed(service,'CSERVICE','InvoicePortal')  ) | \
        (check_transformed(status,'CSTATUS','PENDING') & check_transformed(service,'CSERVICE','IDS')  ) | \
        check_transformed(status,'CSTATUS','SUCCESS') | \
        check_transformed(status,'CSTATUS','SUCCESS_DOWNLOADED') | check_transformed(status,'CSTATUS','SUCCESS_POLLQUEUE')  ))
    return error
"""    


# ### encode value

# In[ ]:


import numpy as np
def etransform(value,column=None, npy= share_directory + 'sla/' + version_sla + '/npy'):
    np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)
    _encoder = encoder.TolerantLabelEncoder(ignore_unknown=True)
    _encoder.classes_ = np.load(npy + '/' + column + '.npy')
    return int( _encoder.transform([value])[0])


# ### decode value

# In[ ]:


def inverse_transform(value,column=None, npy= share_directory + 'sla/' + version_sla + '/npy'):
    _encoder = encoder.TolerantLabelEncoder(ignore_unknown=True)
    _encoder.classes_ = np.load(npy + '/' + column + '.npy')
    if type(value) == int:
        return str(_encoder.inverse_transform(value))  
    elif type(value) == list:
        return [str(_encoder.inverse_transform(v)) for v in value]
    else:
        return None


# In[ ]:


def check_transformed(value0,column,value):
    return value0 == etransform(value,column=column)

def calc_error(columns):
    status = columns[0]
    service = columns[1]
    error=int(not( ( check_transformed(status,'CSTATUS','PENDING') & check_transformed(service,'CSERVICE','InvoicePortal')  ) |         (check_transformed(status,'CSTATUS','PENDING') & check_transformed(service,'CSERVICE','IDS')  ) |         check_transformed(status,'CSTATUS','SUCCESS') |         check_transformed(status,'CSTATUS','SUCCESS_DOWNLOADED') | check_transformed(status,'CSTATUS','SUCCESS_POLLQUEUE')  ))
    return error


# In[9]:


import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
udf_add_error = udf(lambda y,z: calc_error((y,z)), IntegerType())


# In[ ]:


def encode_columns_spark(dataframe=None,columns=None, encoders=None):
    for column in columns:
        #print(column)
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
    df3 = encode_columns_spark(dataframe=dataframe,columns=columns, encoders=encoders)
    #df3 = df3.withColumn("year", udf_add_year(df3.CSTARTTIME)).withColumn("month", udf_add_month(df3.CSTARTTIME)).withColumn("day", udf_add_day(df3.CSTARTTIME)).withColumn("hour", udf_add_hour(df3.CSTARTTIME)).withColumn("minute", udf_add_minute(df3.CSTARTTIME)) 
    df3=cast_spark_columns(dataframe=df3, columns=['CSTARTTIME', 'CENDTIME','CINBOUNDSIZE','CSLATAT','CMESSAGETAT2','CSLADELIVERYTIME'], type='long')
    return df3


# In[ ]:


#_encoder.transform_version(['None'])
#_encoder.transform([None])


# ## update

# In[ ]:


def process_update(year,month,df,encoders=None, columns=None):
    df2 = df.where(f.col("year").isin([year])).where(f.col("month").isin([month]))
    df3 = process(dataframe=df2, encoders=encoders, columns=columns)
    df4 = df3.withColumn("error", udf_add_error(f.col("CSTATUS"), f.col("CSERVICE")).cast(IntegerType()))
    df4.write.mode('overwrite').parquet('/home/jovyan/work/output/v00003_v00001/sla_enc_v00003_v00001_' + str(year) + '_' + str(month) + '.parquet')


# In[ ]:


#!mkdir /home/jovyan/work/output/v00003_v00000
#!ls /home/jovyan/work/output/v00003_v00000


# In[ ]:


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


# In[ ]:


#_df3 = encode_columns_spark(dataframe=df_update_1,columns=columns, encoders=encoders)
#_df3.head()


# In[ ]:


#df_update_2 = process(dataframe=df_update_1, encoders=encoders, columns=columns)
#df_update_2.head()


# In[ ]:


#date(z).date().year
#df2 = df.where(f.col("CSTARTTIME").isin([year]))


# In[ ]:


"""
year = 2019
month = 11
#process_update(year,month,df,encoders=encoders, columns=columns) 


df2 = df.where(f.col("year").isin([year])).where(f.col("month").isin([month]))
df3 = process(dataframe=df2, encoders=encoders, columns=columns)
df4 = df3.withColumn("error", udf_add_error(f.col("CSTATUS"), f.col("CSERVICE")).cast(IntegerType()))
#df4.write.mode('overwrite').parquet('/home/jovyan/work/output/v00003_v00001/sla_enc_v00003_v00001_' + str(year) + '_' + str(month) + '.parquet')
"""


# In[ ]:


#df4.limit(1).toPandas()
#df3.limit(1).toPandas()
#encoders,columns
#_df3 = encode_columns_spark(dataframe=df2,columns=columns, encoders=encoders)


# In[ ]:


#_df3.limit(1).toPandas()


# In[20]:


def get_year_weeks(df):
    columns = ['year','week']
    return df.select(columns).dropDuplicates().toPandas() 

def get_weeks(pf,year):
    l =  list(pf[pf['year'] == year]['week'])
    l.sort()
    return l

pf_year_weeks = get_year_weeks(df)


# In[37]:


"""
df2 = df.where(f.col("year").isin([2023])).where(f.col("week").isin([52]))
pdf2 = df2.toPandas()
pdf2
"""


# In[ ]:


years = [2019,2020,2021,2022,2023]
for year in years:
    weeks = get_weeks(pf_year_weeks,year)
    for week in weeks:
        process_update(year,week,df,encoders=encoders, columns=columns)           

