#!/usr/bin/env python
# coding: utf-8

# In[13]:


import dfBasics
#import common
#import encoder
#import pfAdapt
#import charts


# In[14]:


import pandas as pd
from pyspark.sql import functions
from pyspark.sql.functions import when, lit, col


# In[15]:


sparkSession = dfBasics.getSparkSession()


# In[16]:


df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/user/admin/sla/sla_1580199488133.parquet')


# In[17]:





# In[9]:


for sender in range(0, 2460):
    #df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/user/admin/sla/sla_*.parquet').select(['CGLOBALMESSAGEID', 'year', 'month', 'day', 'hour']).filter(col('CSENDERENDPOINTID')==sender)
    df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/user/admin/sla/sla_*.parquet').select(['CGLOBALMESSAGEID', 'year', 'month', 'day', 'hour','CSTATUS','CSERVICE', 'CSENDERPROTOCOL', 'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT', 'CMESSAGETAT2', 'CSLADELIVERYTIME']).filter(col('CSENDERENDPOINTID')==sender)
    pfall = df.toPandas()
    pfall.to_parquet('/tmp/CSENDERENDPOINTID_' + str(sender) + '.parquet', engine='pyarrow', compression='GZIP')

