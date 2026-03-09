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


#df = sparkSession.read.parquet("/tmp/sla.parquet")
##df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/sla_sql_data/*/*').select(columns).dropDuplicates() 
df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/sla_sql_data/*/*').select(columns)
#senders = pd.read_parquet('/tmp/senders' + '.parquet', engine='pyarrow')


# In[6]:


"""
senders = sparkSession.read.parquet("/tmp/senders.parquet")
#senders = pd.read_parquet('/tmp/senders' + '.parquet', engine='pyarrow')
senders = list(senders.toPandas()['CSENDERENDPOINTID'])
"""

sender_receivers_df = pd.read_parquet('/home/jovyan/work/output/v00004/single/' + 'sender_receivers.parquet')
#sender_receivers_df


# In[7]:


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def transform(value):
    try:
        return int( _encoder.transform([value])[0])
    except Exception as e:
        return -1
    
udf_transform = udf(lambda z: transform(z), StringType())

#df2.withColumn("CSENDERENDPOINTID", str( _encoder.transform([df2.CSENDERENDPOINTID])[0])) 
#df2 = df2.withColumn("CSENDERENDPOINTID", udf_transform(df2.CSENDERENDPOINTID)) 

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


# In[8]:


#!ls /home/jovyan/work/npy


# In[9]:


# ## encode columns

from pyspark.sql.functions import udf
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

from pyspark.sql import functions as F


def process_0(sender=None,receiver=None, dataframe=None,year=None):
    df3 = dataframe.withColumn("timestamp", F.from_unixtime(dataframe.CSTARTTIME / 1000))

    # Step 2: Extract the year from the timestamp
    df4 = df3.withColumn("tyear", F.year("timestamp"))

    # Step 3: Filter the DataFrame using the specified conditions
    df5 = df4.where(
        (F.col("tyear").isin([year])) &
        (F.col("CSENDERENDPOINTID").isin([sender])) &
        (F.col("CRECEIVERENDPOINTID").isin([receiver]))
    )
    
    df6 = df5.fillna(-1)
    
    df7 = encode_columns_spark(dataframe=df6,columns=columns)
    df8 = df7.withColumn("year", udf_add_year(df7.CSTARTTIME)).withColumn("month", udf_add_month(df7.CSTARTTIME)).withColumn("day", udf_add_day(df7.CSTARTTIME)).withColumn("hour", udf_add_hour(df7.CSTARTTIME)).withColumn("minute", udf_add_minute(df7.CSTARTTIME)) 
    df9=cast_spark_columns(dataframe=df8, columns=['CSTARTTIME', 'CENDTIME','CINBOUNDSIZE','CSLATAT','CMESSAGETAT2','CSLADELIVERYTIME'], type='long')
    return df9


def process(sender=None,receiver=None, dataframe=None,year=None):
    #df3 = dataframe.withColumn("timestamp", F.from_unixtime(dataframe.CSTARTTIME / 1000))
    df4 = dataframe
    
    # Step 2: Extract the year from the timestamp
    #df4 = df3.withColumn("tyear", F.year("timestamp"))

    # Step 3: Filter the DataFrame using the specified conditions
    df5 = df4.where(
        (F.col("tyear").isin([year])) &
        (F.col("CSENDERENDPOINTID").isin([sender])) &
        (F.col("CRECEIVERENDPOINTID").isin([receiver]))
    ).persist()
    
    df6 = df5.fillna(-1)
    
    df7 = encode_columns_spark(dataframe=df6,columns=columns)
    df8 = df7.withColumn("year", udf_add_year(df7.CSTARTTIME)).withColumn("month", udf_add_month(df7.CSTARTTIME)).withColumn("day", udf_add_day(df7.CSTARTTIME)).withColumn("hour", udf_add_hour(df7.CSTARTTIME)).withColumn("minute", udf_add_minute(df7.CSTARTTIME)) 
    df9=cast_spark_columns(dataframe=df8, columns=['CSTARTTIME', 'CENDTIME','CINBOUNDSIZE','CSLATAT','CMESSAGETAT2','CSLADELIVERYTIME'], type='long')
    return df9


def process_0(sender=None, receiver=None, dataframe=None, year=None):
    from pyspark.sql.functions import col, year as spark_year, month, dayofmonth, hour, minute, when

    # Filter data as early as possible
    df_filtered = dataframe.where(
        (col("tyear") == year) &
        (col("CSENDERENDPOINTID") == sender) &
        (col("CRECEIVERENDPOINTID") == receiver)
    )
    
    # Minimize transformations by using a single pass with select and when conditions for filling nulls
    df_transformed = df_filtered.fillna(-1).select(
        '*',  # Keep all columns or select specific ones if needed for optimization
        spark_year("CSTARTTIME").alias("year"),
        month("CSTARTTIME").alias("month"),
        dayofmonth("CSTARTTIME").alias("day"),
        hour("CSTARTTIME").alias("hour"),
        minute("CSTARTTIME").alias("minute")
    )
    
    # Encode columns (assuming this is efficient, ensure this function is optimized)
    df_encoded = encode_columns_spark(dataframe=df_transformed, columns=columns)
    
    # Perform type casting (try to do this minimally if possible)
    df_final = cast_spark_columns(
        dataframe=df_encoded, 
        columns=['CSTARTTIME', 'CENDTIME', 'CINBOUNDSIZE', 'CSLATAT', 'CMESSAGETAT2', 'CSLADELIVERYTIME'], 
        type='long'
    )
    
    return df_final


# In[18]:


#import pyspark.sql.functions as f
#ender = senders[0]
#df4 = process(sender=sender,dataframe=df)
#df4.head()
#!mkdir /tmp/enc


# In[19]:


# In[10]:


from pyspark.sql import functions as F
from pyspark.sql.types import LongType

def process_2(sender=None, receiver=None, dataframe=None, year=None):
    # Step 1: Extract timestamp and year in a single transformation
    df3 = (
        dataframe
        .withColumn("timestamp", F.from_unixtime(F.col("CSTARTTIME") / 1000))
        .withColumn("tyear", F.year(F.col("timestamp")))
    )

    # Step 2: Filter the DataFrame using the specified conditions
    if sender is not None:
        df3 = df3.filter(F.col("CSENDERENDPOINTID").isin(sender))
    if receiver is not None:
        df3 = df3.filter(F.col("CRECEIVERENDPOINTID").isin(receiver))
    if year is not None:
        df3 = df3.filter(F.col("tyear") == year)

    # Step 3: Fill NaN values with -1
    df3 = df3.fillna(-1)

    # Step 4: Encode columns (assuming 'columns' is predefined or passed in)
    df3 = encode_columns_spark(dataframe=df3, columns=columns)
    
    # Step 5: Extract date parts in a single transformation
    df3 = (
        df3.withColumn("year", udf_add_year(F.col("CSTARTTIME")))
           .withColumn("month", udf_add_month(F.col("CSTARTTIME")))
           .withColumn("day", udf_add_day(F.col("CSTARTTIME")))
           .withColumn("hour", udf_add_hour(F.col("CSTARTTIME")))
           .withColumn("minute", udf_add_minute(F.col("CSTARTTIME")))
    )

    # Step 6: Cast columns to 'long' type in one go
    long_columns = ['CSTARTTIME', 'CENDTIME', 'CINBOUNDSIZE', 'CSLATAT', 'CMESSAGETAT2', 'CSLADELIVERYTIME']
    df3 = df3.select(
        *df3.columns,
        *[F.col(col).cast(LongType()).alias(col) for col in long_columns]
    )

    return df3


# In[11]:


# In[17]:

np_load_old = np.load

# modify the default parameters of np.load
np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)

# restore np.load for future normal usage
#np.load = np_load_old


# # Main

# In[12]:


import dfBasics
import pandas as pd

version_sla = 'v00004'
version     = version_sla + '/v00000'

home_directory  =  '/home/jovyan/work/'
share_directory =  '/home/jovyan/work/share/'
share_directory = '/home/jovyan/work/output/'
#share_directory =  '/home/jovyan/share/'


# ### encode value

# In[13]:


def e_transform(value,_encoder ):
    try:
        return int( _encoder.transform([value])[0])
    except Exception as e:
        #print(value)
        return -1


# ### decode value

# In[14]:


import numpy
def e_inverse_transform(value,_encoder):
    if type(value) in [int,numpy.int64]:
        return str(_encoder.inverse_transform(value))  
    elif type(value) == list:
        return [str(_encoder.inverse_transform(v)) for v in value]
    else:
        return None


# In[15]:


#!mkdir -p /home/jovyan/work/output/enc


# In[16]:


"""
if None in senders:
    senders.remove(None)
"""


# In[17]:


ENCODED_PATH = '/home/jovyan/work/output/v00004/v00000/encoded/parts/'
NPY_PATH = '/home/jovyan/work/output/v00004/npy/'


# In[18]:


from os import listdir

def listdirectory(path=None,filter='.'):
    return [x for x in listdir(path) if not x.startswith(filter)]    

_files = listdirectory(path=ENCODED_PATH)
#senders = senders[len(_files):]

columns = ['CSTATUS','CSERVICE','CSENDERENDPOINTID','CSENDERPROTOCOL','CRECEIVERPROTOCOL','CRECEIVERENDPOINTID']


# In[19]:


npy=NPY_PATH
encoders = {}
for column in columns:
    _encoder = encoder.TolerantLabelEncoder(ignore_unknown=True)
    _encoder.classes_ = np.load(npy + '/' + column + '.npy')
    encoders[column] = _encoder


# In[21]:


len(sender_receivers_df)


# In[ ]:


import pyspark.sql.functions as f
import os.path
years = [2019,2020,2021,2022,2023,2024]

df3 = df.withColumn("timestamp", F.from_unixtime(df.CSTARTTIME / 1000))
# Step 2: Extract the year from the timestamp
df4 = df3.withColumn("tyear", F.year("timestamp"))
    
for index, row in sender_receivers_df.iloc[8000:].iterrows():
    enc_sender = index
    enc_receivers = list(row['CRECEIVERENDPOINTID'])
    
    sender = e_inverse_transform(enc_sender,encoders['CSENDERENDPOINTID'])
    for enc_receiver in enc_receivers:
        receiver = e_inverse_transform(enc_receiver,encoders['CRECEIVERENDPOINTID'])
        #print(sender,receiver)
        for year in years:
            filename_1 = ENCODED_PATH + "sla_enc_%s_%s_%s_%s_%s_%s.parquet" % ('srfull','v00004_v00000',sender,receiver,'0','0')
            filename = ENCODED_PATH + "sla_enc_%s_%s_%s_%s_%s_%s.parquet" % ('srfull','v00004_v00000',sender,receiver,year,'0')
            
            if not os.path.isfile(filename_1 + '/_SUCCESS'): 
                #print(filename_1)
                if not os.path.isfile(filename + '/_SUCCESS'): 
                    #print(filename)
                    df5 = process(sender=sender,receiver=receiver,dataframe=df4,year=year)
                    df5.write.mode("overwrite").parquet(filename)


# In[ ]:


#e_transform('772e6440-e973-11e8-be62-528eac1b495c',encoders['CSENDERENDPOINTID'] ),enc_sender


# In[ ]:


#df4.head(100)


# In[ ]:


"""
import pyspark.sql.functions as f
import os.path

sender = e_inverse_transform(enc_sender,encoders['CSENDERENDPOINTID'])
for enc_receiver in enc_receivers:
    receiver = e_inverse_transform(enc_receiver,encoders['CRECEIVERENDPOINTID'])
    print(sender,receiver)
    filename = ENCODED_PATH + "sla_enc_%s_%s_%s_%s_%s_%s.parquet" % ('srfull','v00004_v00000',sender,receiver,'0','0')
    if not os.path.isfile(filename + '/_SUCCESS'): 
        df4 = process(sender=sender,receiver=receiver,dataframe=df)
        df4.write.mode("overwrite").parquet(filename)
"""


# In[ ]:


"""
import pyspark.sql.functions as f
#sender = senders[0]
for sender in senders:
    df4 = process(sender=sender,dataframe=df)
    df4.write.mode("overwrite").parquet("/home/jovyan/work/output/enc/sla_enc_" + sender + ".parquet")
"""


# In[ ]:





# In[ ]:




