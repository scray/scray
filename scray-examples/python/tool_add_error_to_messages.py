#!/usr/bin/env python
# coding: utf-8

# # tool_merge_encoded_yearly_messages_all_senders

# # Main

# In[1]:


import dfBasics
import common
import encoder
import pfAdapt
#import charts


# In[2]:


version_sla = 'v00001'
version     = version_sla + '/v00000'

home_directory  =  '/home/jovyan/work/'
share_directory =  '/home/jovyan/work/share/'
#share_directory =  '/home/jovyan/share/'


# In[3]:


import pandas as pd
from pyspark.sql import functions


# ## load dataframe : v00001_v00001

# In[4]:


sparkSession = dfBasics.getSparkSession()
df = sparkSession.read.parquet('/home/jovyan/work/output/sla_enc_all_v00001_v00001.parquet')


# ## encoded values

# ## init encoders (run only once)

# In[5]:


# - run this paragraph only once !
# - if you get an 'allow_pickle' error you need Kernel/restart kernel
import numpy as np
import encoder

np_load_old = np.load

# modify the default parameters of np.load
np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)

# restore np.load for future normal usage
#np.load = np_load_old


# ### encode value

# In[6]:


import numpy as np
def transform(value,column=None, npy= share_directory + 'sla/' + version_sla + '/npy'):
    np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)
    _encoder = encoder.TolerantLabelEncoder(ignore_unknown=True)
    _encoder.classes_ = np.load(npy + '/' + column + '.npy')
    return int( _encoder.transform([value])[0])


# ### decode value

# In[7]:


def inverse_transform(value,column=None, npy= share_directory + 'sla/' + version_sla + '/npy'):
    _encoder = encoder.TolerantLabelEncoder(ignore_unknown=True)
    _encoder.classes_ = np.load(npy + '/' + column + '.npy')
    if type(value) == int:
        return str(_encoder.inverse_transform(value))  
    elif type(value) == list:
        return [str(_encoder.inverse_transform(v)) for v in value]
    else:
        return None


# In[8]:


def check_transformed(value0,column,value):
    return value0 == transform(value,column=column)

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


# In[10]:


df3 = df.withColumn("error", udf_add_error(f.col("CSTATUS"), f.col("CSERVICE")).cast(IntegerType()))


# ## store updated dataframe including errors

# In[1]:


def process(year,month):
    df4 = df3.where(f.col("year").isin([year])).where(f.col("month").isin([month]))
    df4.write.mode('overwrite').parquet('/home/jovyan/work/output/sla_enc_all_v00001_v00002_' + str(year) + '_' + str(month) + '.parquet')


# In[ ]:


years = [2021,2022]
for year in years:
    for month in range(1,13):
        process(year,month)        


# In[ ]:


year = 2019
for month in range(10,13):
        process(year,month)     


# In[ ]:


year = 2023
for month in range(1,7):
        process(year,month)     


# In[ ]:


#df3.write.mode('overwrite').parquet('/home/jovyan/work/output/sla_enc_all_v00001_v00002.parquet')

