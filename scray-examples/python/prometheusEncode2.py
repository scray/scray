#!/usr/bin/env python
# coding: utf-8

# In[3]:


cycle = 15


# # Functions

# In[ ]:


from datetime import time
import datetime as dt

def adddatecolumns(data,pf,column) :
    data['year'] = pf[column].apply(lambda x: x.date().year)
    data['month'] = pf[column].apply(lambda x: x.date().month)
    data['day'] = pf[column].apply(lambda x: x.date().day)
    data['hour'] = pf[column].apply(lambda x: x.time().hour)
    data['minute'] = pf[column].apply(lambda x: x.time().minute)
    #data['second'] = pf[column].apply(lambda x: x.time().second)
    #data['microsecond'] = pf[column].apply(lambda x: x.time().microsecond)

def converttimestampcolumnn(pf,tsc) :
    pf[tsc] = pf[tsc].apply(lambda x: dt.datetime.fromtimestamp(float(x) / 1e3))


# In[ ]:


## Setup charts
import pandas as pd
pd.plotting.register_matplotlib_converters()
import matplotlib.pyplot as plt
#%matplotlib inline
import seaborn as sns
print("Setup Complete")

#vis functions

def label(graph,skip,rot) :
    for ind, label in enumerate(graph.get_xticklabels()):
        if ind % skip == 0:  # every 10th label is kept
            label.set_visible(True)
            label.set_rotation(rot)
        else:
            label.set_visible(False)
            
def abc(mdcountsall) :
    a = mdcountsall.index.get_level_values(0).astype(str)
    b = mdcountsall.index.get_level_values(1).astype(str)
    c = mdcountsall.index.get_level_values(2).astype(str)
    return a + "-" + b + "-" + c 

def abcd(mdcountsall) :
    return abc(mdcountsall) + "-" + mdcountsall.index.get_level_values(3).astype(str)

def ymdh(mdcountsall) :
    return abcd(mdcountsall)

def get_ymdh(mdcountsall) :
    a = mdcountsall.index.get_level_values(0).astype(str)
    b = mdcountsall.index.get_level_values(1).astype(str)
    c = mdcountsall.index.get_level_values(2).astype(str)
    d = mdcountsall.index.get_level_values(3).astype(str)
    return a,b,c,d

def get_ymdh_string(a,b,c,d) :
    return a + "-" + b + "-" + c + "-" + d

def get_ym(mdcountsall) :
    a = mdcountsall.index.get_level_values(0).astype(str)
    b = mdcountsall.index.get_level_values(1).astype(str)
    return a,b

def get_ym_string(a,b) :
    return a + "-" + b


# In[ ]:



#mdcountsall=pfall[(pfall['month'] == 2) & (pfall['day'] > 16)].groupby(['year','month','day','hour'])['outcome'].count()
#mdcountsall=pfall[(pfall['month'] == 2)].groupby(['year','month','day','hour'])['outcome'].count()
#mdcountsall=pfall.groupby(['year','month','day'])['outcome'].count()

def createData(pfall,month,outcome) :
    if outcome < 2 :
        mdcountsall = pfall[(pfall['month'] == month) & (pfall['outcome'] == outcome)].groupby(['year','month','day','hour'])['outcome'].count()
    else :
        if (month > 0) & (month < 13) :
            mdcountsall = pfall[(pfall['month'] == month)].groupby(['year','month','day','hour'])['outcome'].count()
        else :
            mdcountsall = pfall.groupby(['year','month','day','hour'])['outcome'].count()    
    a,b,c,d = get_ymdh(mdcountsall)
    data2 = pd.DataFrame()
    data2['date'] = get_ymdh_string(a,b,c,d)
    data2['outcome'] =  mdcountsall.reset_index()['outcome'].astype(int) 

    #for pivot table
    data2['hours'] =  d.astype(int) 
    data2['days']  =  c.astype(int) 
    return data2

def createData_ym(pfall,month,outcome) :
    if outcome < 2 :
        mdcountsall = pfall[(pfall['outcome'] == outcome)].groupby(['year','month'])['outcome'].count()
    else :
        if (month > 0) & (month < 13) :
            mdcountsall = pfall[(pfall['month'] == month)].groupby(['year','month'])['outcome'].count()
        else :
            mdcountsall = pfall.groupby(['year','month'])['outcome'].count()    
    a,b = get_ym(mdcountsall)
    data2 = pd.DataFrame()
    data2['date'] = get_ym_string(a,b)
    data2['outcome'] =  mdcountsall.reset_index()['outcome'].astype(int) 

    #for pivot table
    #data2['hours'] =  d.astype(int) 
    #data2['days']  =  c.astype(int) 
    return data2

## heatmap
def createHeatmap(piv,title="") :
    plt.figure(figsize=(24,8))
    plt.title(title)
    ax = sns.heatmap(piv, square=True)
    plt.setp( ax.xaxis.get_majorticklabels(), rotation=0 )
    plt.tight_layout()
    plt.show()
    return ax

def createBarplot(md,fx,fy,fontscale,title="") :
    sns.set(style='whitegrid', palette='muted', font_scale=fontscale)
    plt.figure(figsize=(fx,fy))
    plt.title(title)
    ax = sns.barplot(x=md['date'], y=md['outcome'], data=md)
    plt.setp( ax.xaxis.get_majorticklabels(), rotation=0 )
    plt.tight_layout()
    plt.show()
    return ax

# kernel density estimate (KDE) 
def createKDE(data2,fx,fy,fontscale,title="") :
    sns.set(style='whitegrid', palette='muted', font_scale=fontscale)
    plt.figure(figsize=(fx,fy))
    plt.title(title)
    # Histogram 
    #ax = sns.distplot(a=data2['outcome'], kde=False)
    ax = sns.kdeplot(data=data2['outcome'], shade=True)
    plt.tight_layout()
    plt.show()
    return ax


# In[ ]:


def createLineplot(md,fx,fy,fontscale,title="",skip=0,rot=90) :
    sns.set(style='whitegrid', palette='muted', font_scale=fontscale)
    plt.figure(figsize=(fx,fy))
    plt.title(title)
    ax = sns.lineplot(x=md['date'], y=md['value'], data=md)
    for ind, label in enumerate(ax.get_xticklabels()):
        if ind % skip == 0:  # every 10th label is kept
            label.set_visible(True)
            label.set_rotation(rot)
        else:
            label.set_visible(False)
    #plt.setp( ax.xaxis.get_majorticklabels(), rotation=90 )
    plt.tight_layout()
    plt.show()
    return ax

#ax = createLineplot(pfall,16,10,1.4,title="")
#label(ax,500,80)


# Time Series Anomaly Detection with LSTM Autoencoders

# In[ ]:


import numpy as np
#import tensorflow as tf
#from tensorflow import keras
import pandas as pd
import seaborn as sns
from pylab import rcParams
import matplotlib.pyplot as plt
from matplotlib import rc
from pandas.plotting import register_matplotlib_converters

#%matplotlib inline
#%config InlineBackend.figure_format='retina'

register_matplotlib_converters()
sns.set(style='whitegrid', palette='muted', font_scale=1.5)

rcParams['figure.figsize'] = 22, 10

RANDOM_SEED = 42

np.random.seed(RANDOM_SEED)
#tf.random.set_seed(RANDOM_SEED)


# In[ ]:


# functions

OUTCOME = 'close'

TIME_STEPS = 24
#TIME_STEPS = 30
#TIME_STEPS = 720
#TIME_STEPS = 168
#TIME_STEPS = 336

# setup data (current)
def createDataframe(pfall) :
    data3 = createData(pfall,0,2)
    df = pd.DataFrame()
    df[OUTCOME] = data3['outcome']
    df.set_index(data3['date'], inplace=True)
    return df

def getTrainAndTest(df,TRAIN_SIZE) :
    train_size = int(len(df) * TRAIN_SIZE)
    test_size = len(df) - train_size
    train, test = df.iloc[0:train_size], df.iloc[train_size:len(df)]
    print("train.shape: ",train.shape, "test.shape: ", test.shape)
    return train, test

def create_dataset(X, y, time_steps=1):
    Xs, ys = [], []
    for i in range(len(X) - time_steps):
        v = X.iloc[i:(i + time_steps)].values
        Xs.append(v)        
        ys.append(y.iloc[i + time_steps])
    return np.array(Xs), np.array(ys)

def initmodel():
    model = keras.Sequential()
    model.add(keras.layers.LSTM(
        units=64, 
        input_shape=(X_train.shape[1], X_train.shape[2])
    ))
    model.add(keras.layers.Dropout(rate=0.2))
    model.add(keras.layers.RepeatVector(n=X_train.shape[1]))
    model.add(keras.layers.LSTM(units=64, return_sequences=True))
    model.add(keras.layers.Dropout(rate=0.2))
    model.add(keras.layers.TimeDistributed(keras.layers.Dense(units=X_train.shape[2])))
    model.compile(loss='mae', optimizer='adam')
    return model

def testScoreDF(model, THRESHOLD) : 
    X_test_pred = model.predict(X_test)
    test_mae_loss = np.mean(np.abs(X_test_pred - X_test), axis=1)

    test_score_df = pd.DataFrame(index=test[TIME_STEPS:].index)
    test_score_df['loss'] = test_mae_loss
    test_score_df['threshold'] = THRESHOLD
    test_score_df['anomaly'] = test_score_df.loss > test_score_df.threshold
    test_score_df[OUTCOME] = test[TIME_STEPS:][OUTCOME]
    return test_score_df


# In[ ]:


missingSchemaids = [ 1480883705,  -404024316,  -183769575,  2031641327, -1576843338,
                     -660710506,  -208259125,  -650561809,   -39609584, -1115728007]

def withoutMissingSchemas(pf):
    return pf[~pf['schemaid'].isin(missingSchemaids)]

def getHashValues(pf):
    return pd.unique(pf['hashvalue'])

# max timestamp for splits
def getMaxTimestamp(pf):
    return pf.loc[pf['timestamp'].idxmax()]['timestamp']

def getCountDF(pf,column,hashes):
    dft = pd.DataFrame(columns=[column, 'count'])
    i=0
    for hash in hashes:
        pfall=pf[pf[column] == hash]
        num=len(pd.unique(pfall['value']))
        dft.loc[i] = [hash] + [num]
        i=i+1
    return dft.sort_values('count')


# # Start

# In[ ]:


import numpy as np
import pandas as pd

import findspark
findspark.init()

import pyspark
import random

from pyspark import SparkContext
from pyspark.sql import SQLContext

#sc = pyspark.SparkContext(appName="Pi")

#sc = SparkContext()
#sqlContext = SQLContext(sc)


# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

sparkSession = SparkSession.builder.config('spark.local.dir', '/tmp').config("spark.executor.memory", "8g").config("spark.driver.memory", "8g").config("spark.driver.maxResultSize", "0").appName("example-pyspark-read-and-write").getOrCreate()


# In[ ]:


df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/user/admin/stage1/flat_' + str(cycle) + '/*')     .withColumn('timestamp', col('timestamp').cast('long')) 


# # Create encoders

# In[ ]:


#https://stackoverflow.com/questions/50041551/tell-labelenocder-to-ignore-new-labels

import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.utils.validation import check_is_fitted
from sklearn.utils.validation import column_or_1d

class TolerantLabelEncoder(LabelEncoder):
    def __init__(self, ignore_unknown=False,
                       unknown_original_value='unknown', 
                       unknown_encoded_value=-1):
        self.ignore_unknown = ignore_unknown
        self.unknown_original_value = unknown_original_value
        self.unknown_encoded_value = unknown_encoded_value

    def transform(self, y):
        check_is_fitted(self, 'classes_')
        y = column_or_1d(y, warn=True)

        indices = np.isin(y, self.classes_)
        if not self.ignore_unknown and not np.all(indices):
            raise ValueError("y contains new labels: %s" 
                                         % str(np.setdiff1d(y, self.classes_)))

        y_transformed = np.searchsorted(self.classes_, y)
        y_transformed[~indices]=self.unknown_encoded_value
        return y_transformed

    def inverse_transform(self, y):
        check_is_fitted(self, 'classes_')

        labels = np.arange(len(self.classes_))
        indices = np.isin(y, labels)
        if not self.ignore_unknown and not np.all(indices):
            raise ValueError("y contains new labels: %s" 
                                         % str(np.setdiff1d(y, self.classes_)))

        y_transformed = np.asarray(self.classes_[y], dtype=object)
        y_transformed[~indices]=self.unknown_original_value
        return y_transformed


# In[ ]:


def createEncoders(dataall,columns):
    for column in columns:
        le = TolerantLabelEncoder(ignore_unknown=True)
        #le.fit([1, 2, 2, 6])
        le.fit(dataall[column])
        LabelEncoder()
        print(le.classes_)
        np.save(column + '.npy', le.classes_)
        
def encode(dataall,columns):
    # save np.load
    np_load_old = np.load

    # modify the default parameters of np.load
    np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)

    for column in columns:
        encoder = TolerantLabelEncoder(ignore_unknown=True)
        encoder.classes_ = np.load(column + '.npy')
        dataall[column] = encoder.transform(dataall[column]) 

    # restore np.load for future normal usage
    np.load = np_load_old
    
def getEncoder(column):
    # save np.load
    np_load_old = np.load

    # modify the default parameters of np.load
    np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)

    encoder = TolerantLabelEncoder(ignore_unknown=True)
    encoder.classes_ = np.load(column + '.npy')
        
    # restore np.load for future normal usage
    np.load = np_load_old
    return encoder


# In[ ]:


# split instance name

def igroup(x):
    if x[0:3] == 'cls':
        res = x.split('0')
        return res[0]
    return x
    

def inode(x):
    if x[0:3] == 'cls':
        res = x.split('0')
        return res[1].split(':')[0]
    return '1'
    


# # TMP

# In[ ]:


from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

cat_features = ['service', '__name__','instance']

def encodeall(pfall,cat_features):
    #Prepping categorical variables
    from sklearn.preprocessing import LabelEncoder
    encoder = LabelEncoder()
    # Apply the label encoder to each column
    encodedpfall = pfall[cat_features].apply(encoder.fit_transform)
    return encodedpfall
  
def getpfbasic(pfall) :   
    #pfall = df2.limit(5000000).toPandas()    
    #print(pfall.loc[pfall['CSTARTTIME'].idxmax()]['CSTARTTIME'],len(pfall.index))
    pfall = pfall.assign(outcome=(~( ((pfall['CSTATUS'] == 'PENDING') & (pfall['CSERVICE'] == 'InvoicePortal')) | ((pfall['CSTATUS'] == 'PENDING') & (pfall['CSERVICE'] == 'IDS')) | (pfall['CSTATUS'] == 'SUCCESS') | (pfall['CSTATUS'] == 'SUCCESS_DOWNLOADED') | (pfall['CSTATUS'] == 'SUCCESS_POLLQUEUE'))).astype(int))
    converttimestampcolumnn(pfall,'CSTARTTIME')
    pfall['CGLOBALMESSAGEID'] = pfall['CGLOBALMESSAGEID'].apply(hash)
    #pfall['CSENDERENDPOINTID'] = pfall['CSENDERENDPOINTID'].astype(str)   
    #pfall['CRECEIVERENDPOINTID'] = pfall['CRECEIVERENDPOINTID'].astype(str)
    pfall['CMESSAGETAT2'] = pfall['CMESSAGETAT2'].astype(int)
    pfall['CSLATAT'] = pfall['CSLATAT'].astype(int)
    pfall['CINBOUNDSIZE'] = pfall['CINBOUNDSIZE'].astype(int)
    return pfall

def getpf(df2) :    
    pfall = getpfbasic(df2)
    encodedpfall = encodeall(pfall,cat_features)
    dataall = pfall[['CGLOBALMESSAGEID','CMESSAGETAT2','CSLATAT','CINBOUNDSIZE', 'outcome']].join(encodedpfall)
    adddatecolumns(dataall,pfall)    
    return dataall

def astype(pfall,selected,newtype):
    for each in selected:
        pfall[each] = pfall[each].astype(newtype)


# In[ ]:


import numpy as np
import hashlib
from pyspark.sql.functions import when, lit, col

selected = ['date', 'timestamp', 'value', 'product', 'service', '__name__','instance',  'schemaid', 'hashvalue']
#selected = ['date','timestamp','value','product','service','__name__','instance','job','le','port','schemaid','hashvalue']
#selected = ['timestamp', 'value', 'schemaid', 'hashvalue']

def getpfall(df,selected) :
    #pfall = df.limit(5000000).toPandas()  
    pfall = df.toPandas() 
    pfall['timestamp'] = pfall['timestamp'].astype(int)
    pfall['schemaid'] = pfall['schemaid'].astype(int)
    pfall['hashvalue'] = pfall['hashvalue'].astype(int)
    #pfall['value'] = pfall['value'].astype(float)
    #for each in selected:
    #    pfall[each] = pfall[each].astype(str)
    if len(pfall) == 0:
        return pfall,0,0
    #used = usedcolumns(pfall)
    #keepcolumns(pfall,used)
    return pfall, int(pfall.loc[pfall['timestamp'].astype(int).idxmax()]['timestamp']),len(pfall.index)        

def gettest(to,selected) :
    df2 = df.withColumn('timestamp', col('timestamp').cast('long')).filter(col("timestamp") < to ).select(selected).dropDuplicates().orderBy('timestamp')    
    return getpfall(df2,selected)

def getdata_lt(to,selected=[], filter_names = "") :
    if len(filter_names) > 0:
        print (len(filter_names))
        df2 = df.filter(col("__name__") == filter_names ).withColumn('timestamp', col('timestamp').cast('long')).filter(col("timestamp") < to ).dropDuplicates().orderBy('timestamp') 
    else:
        df2 = df.withColumn('timestamp', col('timestamp').cast('long')).filter(col("timestamp") < to ).dropDuplicates().orderBy('timestamp')  
    #df2 = df.filter(col("__name__") == 'bis_adapter_finished_processes_total' ).withColumn('timestamp', col('timestamp').cast('long')).filter(col("timestamp") < to ).select(selected).dropDuplicates().orderBy('timestamp') 
    #dataall = getpf(df2)    
    #return dataall
    return getpfall(df2,selected)
    #return df2
    

# >=from  <=to    
def getdata_ft(_from,_diff,selected=[], filter_names = "") :
    to = _from + _diff
    if len(filter_names) == 0:
        print(_from,to)
        #df2 = df.withColumn('timestamp', col('timestamp').cast('long')) \
        df2 = df.filter(col("timestamp") >= _from )                 .filter(col("timestamp") <= to )                 .filter(~df['schemaid'].isin(*missingSchemaids)) 
                #.orderBy('timestamp') 
        print((df2.count(), len(df2.columns)))
    else:    
        df2 = df.filter(col("__name__") == filter_names ).withColumn('timestamp', col('timestamp').cast('long')).filter(col("timestamp") >= _from ).filter(col("timestamp") <= to ).dropDuplicates().orderBy('timestamp') 
    return getpfall(df2,selected)

# >from  <=to   
def getdata_gt(_from,_diff,selected=[], filter_names = "") :
    to = _from + _diff
    if len(filter_names) == 0:
        print(_from,to)
        #df2 = df.withColumn('timestamp', col('timestamp').cast('long')) \
        df2 = df.filter(col("timestamp") > _from )                 .filter(col("timestamp") <= to )                 .filter(~df['schemaid'].isin(*missingSchemaids)) 
                #.orderBy('timestamp') 
        print((df2.count(), len(df2.columns)))
    else:    
        df2 = df.filter(col("__name__") == filter_names ).withColumn('timestamp', col('timestamp').cast('long')).filter(col("timestamp") >= _from ).filter(col("timestamp") <= to ).dropDuplicates().orderBy('timestamp') 
    return getpfall(df2,selected)


# In[ ]:


row1 = df.agg({"timestamp": "min"}).collect()[0]
row2 = df.agg({"timestamp": "max"}).collect()[0]

print (row1)
dfminTimestamp = row1["min(timestamp)"]
print (row2)
dfmaxTimestamp = row2["max(timestamp)"]

dfrowCount    = df.count()
dfcolumnCount = len(df.columns)


# In[ ]:


withoutColumns = ['date', 'timestamp', 'value','instance','schemaid', 'hashvalue']
columns = df.limit(1).toPandas().columns
columns = columns[~columns.isin(withoutColumns)]


# In[ ]:


from datetime import time
import datetime as dt
import calendar
import pytz
de = pytz.timezone('Europe/Berlin')

def date(x):
    return  dt.datetime.fromtimestamp(float(x), tz=de)


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


def encodeDataframe(dataall):
    astype(dataall,columns,str)
    encode(dataall,columns)

    del dataall['date']
    #del dataall['timestamp']
    
    astype(dataall,['instance'],str)
    dataall['igroup'] = dataall['instance'].apply(lambda x: igroup(x))
    dataall['inode']  = dataall['instance'].apply(lambda x: inode(x))

    #createEncoders(dataall,['igroup'])
    encode(dataall,['igroup'])
    astype(dataall,['inode'],int)
    del dataall['instance']

    # convert timestamp to datetime and add column date
    import calendar
    import pytz
    de = pytz.timezone('Europe/Berlin')
    #dataall['date'] = dataall['timestamp'].apply(lambda x: dt.datetime.fromtimestamp(float(x), tz=de))
    adddatecolumns(dataall,dataall,'timestamp')
    #del dataall['date'] 
    return dataall


# In[ ]:


def save(dataall,part,timestamp):
    dataall.to_parquet('/tmp/myfile_' + str(part) +'_' + str(timestamp) + '.parquet', engine='fastparquet', compression='GZIP')


# In[ ]:


def walk(part,timestamp,max_cycles=1,timestamp_diff=1000):
    cycle = 0
    while True:
        #next
        df2 = getdata_gt(timestamp,timestamp_diff)
        encodeDataframe(df2[0])

        #merge and prepare for next step
        #dataall = dataall.append(df2[0], ignore_index=True)
        timestamp = df2[1]
        print(timestamp,df2[2])
        #df2[0].to_parquet('/tmp/myfile_11_' + str(timestamp) + '.parquet', engine='fastparquet', compression='GZIP')
        save(df2[0],part,timestamp)
        
        if df2[2] == 0:
            break  
        cycle = cycle + 1
        if cycle == max_cycles:
            break
    return timestamp


# In[ ]:


def process_ft(part,_from,timestamp_diff):
    #from =  dfminTimestamp
    #timestamp_diff = 2000
    df2 = getdata_ft(_from,timestamp_diff,selected=[], filter_names = "")
    dataall = encodeDataframe(df2[0])
    timestamp = df2[1]
    save(dataall,part,timestamp)
    return timestamp


# In[ ]:

#timestamp = 1578530678
timestamp = process_ft(cycle,dfminTimestamp,3000)
timestamp = walk(cycle,timestamp,10000,1000)

