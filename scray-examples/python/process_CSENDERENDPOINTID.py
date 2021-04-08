#!/usr/bin/env python
# coding: utf-8

# In[4]:


import dfBasics
#import common
#import encoder
#import pfAdapt
#import charts


# In[5]:


import pandas as pd
from pyspark.sql import functions
from pyspark.sql.functions import when, lit, col


# In[6]:


sparkSession = dfBasics.getSparkSession()


# In[9]:


for sender in range(0, 2460):
    df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/user/admin/sla/sla_*.parquet').select(['CGLOBALMESSAGEID', 'year', 'month', 'day', 'hour']).filter(col('CSENDERENDPOINTID')==sender)
    pfall = df.toPandas()
    pfall.to_parquet('/tmp/CSENDERENDPOINTID_' + str(sender) + '.parquet', engine='pyarrow', compression='GZIP')

