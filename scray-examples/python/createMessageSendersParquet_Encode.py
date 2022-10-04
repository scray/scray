#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import dfBasics
import common
import encoder
import pfAdapt
#import charts


# In[ ]:


import pandas as pd
from pyspark.sql import functions


# # Lib functions

# # Main

# In[ ]:


columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE',       'CSLABILLINGMONTH', 'CSENDERPROTOCOL', 'CSENDERENDPOINTID',       'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT',       'CMESSAGETAT2', 'CSLADELIVERYTIME']
# withot 'CSLABILLINGMONTH'
def get_columns_2():
    columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE',            'CSENDERPROTOCOL', 'CSENDERENDPOINTID',           'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT',           'CMESSAGETAT2', 'CSLADELIVERYTIME']
    return columns
#columns = get_columns_2()
#to count messages sent
columns = [ 'CSTARTTIME', 'CSENDERENDPOINTID']


# In[ ]:


sparkSession = dfBasics.getSparkSession()


# In[ ]:


df2 = sparkSession.read.parquet("/tmp/sla.parquet")
#senders = pd.read_parquet('/tmp/senders' + '.parquet', engine='pyarrow')


# In[ ]:


import numpy as np
import encoder

np_load_old = np.load

# modify the default parameters of np.load
np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)

column = 'CSENDERENDPOINTID'
_encoder = encoder.TolerantLabelEncoder(ignore_unknown=True)
_encoder.classes_ = np.load('/home/jovyan/work/cls/jupyter/npy/' + column + '.npy')
#dataall[column] = _encoder.transform(dataall[column]) 

# restore np.load for future normal usage
#np.load = np_load_old


# In[ ]:


from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def transform(value):
    #print(value)
    return str( _encoder.transform([value])[0])
    #return value
    #return '10' + value

udf_transform = udf(lambda z: transform(z), StringType())

#df2.withColumn("CSENDERENDPOINTID", str( _encoder.transform([df2.CSENDERENDPOINTID])[0])) 
df2 = df2.withColumn("CSENDERENDPOINTID", udf_transform(df2.CSENDERENDPOINTID)) 


# In[ ]:


df2.write.parquet("/tmp/sla_enc.parquet")

