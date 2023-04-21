#!/usr/bin/env python
# coding: utf-8

# # tool_merge_encoded_messages_all_senders

# # Main

# In[2]:


import dfBasics
import common
import encoder
import pfAdapt
#import charts


# In[3]:


version_sla = 'v00001'
version     = version_sla + '/v00000'

home_directory  =  '/home/jovyan/work/'
share_directory =  '/home/jovyan/work/share/'
#share_directory =  '/home/jovyan/share/'


# In[4]:


import pandas as pd
from pyspark.sql import functions


# In[5]:


columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE',       'CSLABILLINGMONTH', 'CSENDERPROTOCOL', 'CSENDERENDPOINTID',       'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT',       'CMESSAGETAT2', 'CSLADELIVERYTIME']
# withot 'CSLABILLINGMONTH'
def get_columns_2():
    columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE',            'CSENDERPROTOCOL', 'CSENDERENDPOINTID',           'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT',           'CMESSAGETAT2', 'CSLADELIVERYTIME']
    return columns

columns = ['CGLOBALMESSAGEID',  'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE', 'CSENDERENDPOINTID', 'CSENDERPROTOCOL', 'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT', 'CMESSAGETAT2', 'CSLADELIVERYTIME']
     

#columns = get_columns_2()
#to count messages sent
#columns = [ 'CSTARTTIME', 'CSENDERENDPOINTID']


# In[6]:


sparkSession = dfBasics.getSparkSession()


# In[6]:


df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/sla_sql_data/*/*').select(columns).dropDuplicates() 


# In[ ]:


#senders = sparkSession.read.parquet("/tmp/senders.parquet")
#senders = pd.read_parquet('/tmp/senders' + '.parquet', engine='pyarrow')
#senders = list(senders.toPandas()['CSENDERENDPOINTID'])


# In[7]:


senders = list(sparkSession.read.parquet('hdfs://172.30.17.145:8020/user/admin/sla/v00000/v00000/senders/senders.parquet').toPandas()['CSENDERENDPOINTID'])


# In[8]:


len(senders)


# In[9]:


#senders


# In[10]:


senders_files = sparkSession.read.options(delimiter=',')                       .csv('hdfs://172.30.17.145:8020/user/admin/sla/' + version + '/senders/senders_files.txt').toPandas()
senders_files.columns = ['size','filename']    


# In[10]:


#senders_files['filename'][0]


# In[11]:


'sla_enc_' + senders[0] + '.parquet'


# In[12]:


failed = []
for sender in senders:
    try:
        filename = 'sla_enc_' + sender + '.parquet'
        sparkSession.read.text('hdfs://172.30.17.145:8020/user/admin/sla/' + version + '/encoded/senders/' + filename + '/_SUCCESS')
    except Exception as exception: 
        failed.append(sender)
        #if not 'temporary' in filename:
            #failed.append(filename.split('sla_enc_')[1].split('.')[0])


# In[13]:


failed.remove(None)
len(failed)


# In[14]:


working = [x for x in senders if x not in failed]
working.remove(None)


# In[16]:


working.remove(None)


# In[19]:


len(working)


# In[19]:


sender = working[0]
filename = 'sla_enc_' + sender + '.parquet'
_df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/user/admin/sla/' + version + '/encoded/senders/' + filename ).toPandas()
_df.columns


# In[23]:


columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE', 'CSENDERENDPOINTID', 'CSENDERPROTOCOL', 'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT', 'CMESSAGETAT2', 'CSLADELIVERYTIME', 'year', 'month', 'day', 'hour', 'minute']


# In[25]:


#columns


# In[26]:


df = pd.DataFrame(columns=columns)
#df.append(_df)
#df


# In[ ]:


for sender in working:
    filename = 'sla_enc_' + sender + '.parquet'
    _df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/user/admin/sla/' + version + '/encoded/senders/' + filename ).toPandas()
    df = df.append(_df)


# In[38]:


#!ls -l /home/jovyan/work/output/sla_enc_all.parquet


# In[39]:


df.to_parquet('/home/jovyan/work/output/sla_enc_all_3.parquet')


# In[41]:


#pd.read_parquet('/home/jovyan/work/output/sla_enc_all_2.parquet')


# In[31]:


#_senders = pd.unique(df['CSENDERENDPOINTID'])


# In[32]:


len(_senders)


# In[30]:


#pd.unique(df[df['CSENDERENDPOINTID'] == _senders[0]]['CRECEIVERPROTOCOL'])


# In[50]:


#pd.unique(df['CRECEIVERPROTOCOL'])

