#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[ ]:


import dfBasics
import common
#import encoder
#import pfAdapt
#import charts


# In[ ]:


import pandas as pd
from pyspark.sql import functions
import pyspark.sql.functions as f


# In[ ]:


sparkSession = dfBasics.getSparkSession()


# In[ ]:


#df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/sla_sql_data/*/*').select(columns).dropDuplicates() 
df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/sla_sql_data/*/*')


# In[ ]:


senders = pd.read_parquet('/home/jovyan/work/output/v00004/single/senders.parquet')
receivers = pd.read_parquet('/home/jovyan/work/output/v00004/single/receivers.parquet')


# In[ ]:


def get_columns():
    return sparkSession.read.parquet('hdfs://172.30.17.145:8020/sla_sql_data/*/*').limit(1).toPandas().columns

def get_senders(df):
    return df.select(['CSENDERENDPOINTID']).dropDuplicates().toPandas()

def get_receivers(df):
    return df.select(['CRECEIVERENDPOINTID']).dropDuplicates().toPandas()

columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE',
       'CSLABILLINGMONTH', 'CSENDERPROTOCOL', 'CSENDERENDPOINTID',
       'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT',
       'CMESSAGETAT2', 'CSLADELIVERYTIME']


# In[ ]:


import os.path

for sender in list(senders['CSENDERENDPOINTID']):
    try:
        filename = '/home/jovyan/work/output/v00004/single/sender_receivers/' + sender + '.parquet'
        if not os.path.isfile(filename): 
            df3 = df.where(f.col("CSENDERENDPOINTID").isin([sender]))
            r = get_receivers(df3)
            r.to_parquet(filename)
    except Exception as e:
        print(e,sender)

