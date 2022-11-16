#!/usr/bin/env python
# coding: utf-8

# # Base for Tools to encode messages (current)

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


# ## work

# In[ ]:


df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/sla_sql_data/*/*').select(['CSENDERENDPOINTID']).dropDuplicates() 


# In[ ]:


df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/sla_sql_data/*/*').select(columns).dropDuplicates() 


# In[ ]:


#!rm -rf /tmp/sla.parquet
df.write.mode("overwrite").parquet("/tmp/sla.parquet") 


# In[ ]:


df = sparkSession.read.parquet("/tmp/sla.parquet")


# In[ ]:


df.head()  


# In[ ]:





# In[ ]:


# fix null endpoints
from pyspark.sql.functions import when
df2 = df.withColumn("CSENDERENDPOINTID", when(df.CSENDERENDPOINTID.isNull(), "None").otherwise(df.CSENDERENDPOINTID))


# In[ ]:


df2.head()


# In[ ]:


#!rm -rf /tmp/sla.parquet

df2.write.mode("overwrite").parquet("/tmp/sla.parquet") 


# ## create file with current sending endpoints

# In[ ]:


senders = df2.select([ 'CSENDERENDPOINTID']).dropDuplicates().toPandas() 


# In[ ]:


senders.to_parquet('/tmp/senders' + '.parquet', engine='pyarrow', compression='GZIP')


# In[ ]:


senders = sparkSession.read.parquet("/tmp/senders.parquet")
senders = list(senders.toPandas()['CSENDERENDPOINTID'])


# In[ ]:


len(senders)


# ## create column encoders (pyspark)

# In[ ]:


import numpy as np
import encoder

def createEncoders(dataall,columns):
    for column in columns:
        le = encoder.TolerantLabelEncoder(ignore_unknown=True)
        #le.fit([1, 2, 2, 6])
        le.fit(dataall[column])
        encoder.LabelEncoder()
        #print(le.classes_)
        np.save('/home/jovyan/work/cls/jupyter/npy/' + column + '.npy', le.classes_)
        
#!ls /home/jovyan/work/cls/jupyter/npy
#createEncoders(senders,['CSENDERENDPOINTID'])


# In[ ]:


columns = list(df.limit(1).toPandas().columns)
columns.remove('CGLOBALMESSAGEID')
columns.remove('CSLATAT')
columns.remove('CMESSAGETAT2') 
columns.remove('CSLADELIVERYTIME')
columns.remove('CINBOUNDSIZE')


# In[ ]:


for column in columns:
    print (column)
    pfall=df.select([column]).dropDuplicates().toPandas()
    createEncoders(pfall,[column])


# ## list directory

# In[ ]:


from os import listdir

def listdirectory(path=None,filter='.'):
    return [x for x in listdir(path) if not x.startswith(filter)]    

_files = listdirectory(path='/tmp/enc')


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


import pyspark.sql.functions as f
#sender = senders[0]
for sender in senders:
    df4 = process(sender=sender,dataframe=df)
    df4.write.mode("overwrite").parquet("/tmp/enc/sla_enc_" + sender + ".parquet")


# ## work

# In[ ]:


get_ipython().system('mkdir /tmp/enc')


# In[ ]:


get_ipython().system('ls /tmp/sla_enc_772e6440-e973-11e8-be62-528eac1b495c.parquet')


# In[ ]:


#list(df.limit(1).toPandas().columns)

#pfall=df3.toPandas()
#pfall.dtypes


# In[ ]:


#df4 =  df3.withColumn("CSENDERENDPOINTID", udf_transform(df3.CSENDERENDPOINTID).cast("Integer"))


# In[ ]:


#df4.show(10)
get_ipython().system('date')
df4.write.mode("overwrite").parquet("/tmp/sla_enc.parquet")
get_ipython().system('date')


# In[ ]:


np_load_old = np.load

# modify the default parameters of np.load
np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)

# restore np.load for future normal usage
#np.load = np_load_old


# In[ ]:


pfall=df3.toPandas()
pfall


# In[ ]:


#df3 = df3.withColumn("CSTARTTIME", col("CSTARTTIME").cast("decimal(38,0)"))
df3 = df3.withColumn("CSTARTTIME", col("CSTARTTIME").cast("long"))
pfall=df3.toPandas()
pfall.dtypes


# In[ ]:


list(df.limit(1).toPandas().columns).remove('CGLOBALMESSAGEID')


# In[ ]:


#_encoder.transform(['None'])
_encoder.transform(['0014de30-f8ec-11eb-9933-02daac1e124d'])
#!ls /home/jovyan/work/jupyter/npy/    
#!pwd
_encoder.inverse_transform(4866)
#len(_encoder.classes_)


# In[ ]:


df2.count()


# In[ ]:


#df3 =  df2.limit(50000000).withColumn("CSENDERENDPOINTID", udf_transform(df2.CSENDERENDPOINTID).cast("Integer"))
df3 =  df2.withColumn("CSENDERENDPOINTID", udf_transform(df2.CSENDERENDPOINTID).cast("Integer"))


# In[ ]:


get_ipython().system('date')
df3.head()
#df3.show(10)
get_ipython().system('date')


# In[ ]:


get_ipython().system('date')


# In[ ]:


from pyspark.sql.types import IntegerType
df3 = df3.withColumn("CSENDERENDPOINTID", df3["CSENDERENDPOINTID"].cast(IntegerType()))


# In[ ]:


#######
df3.head(11)


# In[ ]:


get_ipython().system('rm -rf /tmp/sla_enc.parquet')


# In[ ]:


df3.write.mode("overwrite").parquet("/tmp/sla_enc.parquet")


# In[ ]:


pfall = df3.toPandas() 


# In[ ]:





# In[ ]:


from datetime import time
import datetime as dt
import calendar
import pytz
de = pytz.timezone('Europe/Berlin')

# long timestamp
def date(x):
    return  dt.datetime.fromtimestamp(float(x) / 1e3, tz=de)


def adddatecolumns(data,pf,column) :
    data['year'] = pf[column].apply(lambda x: date(x).date().year)
    data['month'] = pf[column].apply(lambda x: date(x).date().month)
    data['day'] = pf[column].apply(lambda x: date(x).date().day)
    data['hour'] = pf[column].apply(lambda x: date(x).time().hour)
    data['minute'] = pf[column].apply(lambda x: date(x).time().minute)
    #data['second'] = pf[column].apply(lambda x: x.time().second)
    #data['microsecond'] = pf[column].apply(lambda x: x.time().microsecond)

def converttimestampcolumnn(pf,tsc) :
    pf[tsc] = pf[tsc].apply(lambda x: dt.datetime.fromtimestamp(float(x) / 1e3))


# In[ ]:


def astype(pfall,selected,newtype):
    for each in selected:
        pfall[each] = pfall[each].astype(newtype)


# In[ ]:


selected = [  'CSENDERENDPOINTID']

astype(pfall,selected,str) 
encoder.encode(pfall,selected)
#astype(pfall,['CSTARTTIME','CENDTIME','CSLATAT','CMESSAGETAT2','CSLADELIVERYTIME','CINBOUNDSIZE'] ,int) 
#del(pfall['CSLABILLINGMONTH'])
#pfall['CGLOBALMESSAGEID'] = pfall['CGLOBALMESSAGEID'].apply(hash)
pfall = pfall.drop_duplicates()


# In[ ]:


# convert timestamp to datetime and add column date
#import calendar
##import pytz
#de = pytz.timezone('Europe/Berlin')
adddatecolumns(pfall,pfall,'CSTARTTIME')


# In[ ]:


astype(pfall,['CSTARTTIME'] ,int) 


# In[ ]:


import time; ts = str(time.time()) 
pfall.to_parquet('/tmp/msgsenders_' + ts + '.parquet', engine='fastparquet', compression='GZIP')


# In[ ]:


#!mv /tmp/msgsenders_date.parquet /tmp/msgsenders_200716.parquet
import time; ts = time.time() 
print(ts)

