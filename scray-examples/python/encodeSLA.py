#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import dfBasics
import common
import encoder
import pfAdapt
#import charts


# In[ ]:


def install():
    import os
    os.environ['http_proxy'] = "http://172.30.12.56:3128" 
    os.environ['https_proxy'] = "https://172.30.12.56:3128"  
    get_ipython().system('pip install pyarrow')
    #!pip3 install fastparquet
    #!conda uninstall fastparquet
    #!conda config --add channels conda-forge
    #!conda install -y -c conda-forge fastparquet


# In[ ]:


import pandas as pd
from pyspark.sql import functions


# In[ ]:


def getCountDF(pf,column,hashes):
    dft = pd.DataFrame(columns=[column, 'count'])
    i=0
    for hash in hashes:
        pfall=pf[pf[column] == hash]
        num=len(pfall)
        if num > 1:
            dft.loc[i] = [hash] + [num]
            i=i+1
    return dft
    #return dft.sort_values('count')

def usedcolumns(tb,row):
    col = []
    for column in tb.columns:
        if tb.iloc[row][column] == None :
            col.append(column)
    return col

def diffcolumns(tb):
    col = []
    for column in tb.columns:
        if tb.iloc[0][column] != tb.iloc[1][column] :
            col.append(column)
    return col

def printtt1():    
    col = []
    for index, row in tt.iterrows():
        tb = pfall[pfall['CGLOBALMESSAGEID'] == row['CGLOBALMESSAGEID']]
        for bindex, brow in tb.iterrows():
            if pfall.loc[bindex]['CSLADELIVERYTIME'] < 0 :
                #print (str(index) + ' ' + str(bindex))
                col.append(bindex)
    return col


# In[ ]:


def astype(pfall,selected,newtype):
    for each in selected:
        pfall[each] = pfall[each].astype(newtype)     


# # B

# In[ ]:


selected = [ 'CSTATUS', 'CSERVICE',        'CSENDERPROTOCOL', 'CSENDERENDPOINTID',        'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID']


# In[ ]:


def encodepfall(pfall,number):
    print('1')
    astype(pfall,['CSTARTTIME','CENDTIME','CSLATAT','CMESSAGETAT2','CSLADELIVERYTIME','CINBOUNDSIZE'] ,int) 
    #del(pfall['CSLABILLINGMONTH'])
    pfall['CGLOBALMESSAGEID'] = pfall['CGLOBALMESSAGEID'].apply(hash)
    print('1b')
    ac = pd.unique(pfall['CGLOBALMESSAGEID'])
    print('2')
    tt = getCountDF(pfall,'CGLOBALMESSAGEID',ac)
    print('3')
    col = printtt() 
    print('4')
    pfall = pfall.drop(col)
    print('5')
    astype(pfall,selected,str) 
    encoder.encode(pfall,selected)
    
    print('6')
    pfall = pfall.drop_duplicates()
    print('7')
    pfall.to_parquet('/tmp/sla_' + number + '.parquet', engine='fastparquet', compression='GZIP')
    return pfall


# # Main

# In[ ]:


columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE',       'CSLABILLINGMONTH', 'CSENDERPROTOCOL', 'CSENDERENDPOINTID',       'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT',       'CMESSAGETAT2', 'CSLADELIVERYTIME']
# withot 'CSLABILLINGMONTH'
def get_columns_2():
    columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE',            'CSENDERPROTOCOL', 'CSENDERENDPOINTID',           'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT',           'CMESSAGETAT2', 'CSLADELIVERYTIME']
    return columns
columns = get_columns_2()
#to count messages sent
#columns = [ 'CSTARTTIME', 'CSENDERENDPOINTID']


# In[ ]:


sparkSession = dfBasics.getSparkSession()


# In[ ]:


def printtt(pfall,tt):    
    col = []
    #col2 = []
    i = 0
    for index in tt.index.get_level_values(0):
        tb = pfall[pfall['CGLOBALMESSAGEID'] == index]
        findex = []
        for bindex, brow in tb.iterrows():   
            if pfall.loc[bindex]['CSLADELIVERYTIME'] < 0 :
                #print (str(index) + ' ' + str(bindex))
                #col.append(bindex)
                findex.append(bindex)
        if(len(findex) == 1):
            col.append(findex[0])   
        elif(len(findex) == 2):
            if (pfall.iloc[findex[0]]['CENDTIME'] > pfall.iloc[findex[1]]['CENDTIME']):
                col.append(findex[0])
            else:
                col.append(findex[1])
        else:
            if (tb.iloc[0]['CMESSAGETAT2'] > tb.iloc[1]['CMESSAGETAT2']):
                col.append(tb.index[0])
            else:
                col.append(tb.index[1]) 
    return col


# # slatimestamps

# In[ ]:


filetimestamps = list(sparkSession.read.text('hdfs://172.30.17.145:8020/user/admin/slatimestamps.txt').select('value').toPandas()['value'])


# In[ ]:


def getencodedpfall(line) :
    try:
        #line = filetimestamps[0]
        pfall = sparkSession.read.parquet('hdfs://172.30.17.145:8020/sla_sql_data/' + line +  '/*').select(columns).dropDuplicates().toPandas()
        astype(pfall,['CSTARTTIME','CENDTIME','CSLATAT','CMESSAGETAT2','CSLADELIVERYTIME','CINBOUNDSIZE'] ,int) 
        pfall['CGLOBALMESSAGEID'] = pfall['CGLOBALMESSAGEID'].apply(hash)
        mdcountsall = pfall.groupby(['CGLOBALMESSAGEID','CSTATUS'])['CSTARTTIME'].count()
        mdcountsall = mdcountsall[mdcountsall > 1]
        f = printtt(pfall,mdcountsall)
        pfall = pfall.drop(f)
        astype(pfall,selected,str) 
        encoder.encode(pfall,selected)
        
        # check for duplicates
        g=pfall.groupby('CGLOBALMESSAGEID')['CGLOBALMESSAGEID'].value_counts()
        _duplicates = list(g.where(g>1).dropna().index.get_level_values(0))

        _drop_index = []
        for id in _duplicates:
            index = pfall[pfall['CGLOBALMESSAGEID'] == id].sort_values('CENDTIME').index
            _drop_index.append(index[0])
     
        # drop duplicates    
        pfall = pfall.drop(_drop_index)   
        return pfall
    except Exception as e:
        print("does not exist:" + line)


# In[ ]:


for line in filetimestamps:
    pfall = getencodedpfall(line)
    pfall.to_parquet('/tmp/sla_' + line + '.parquet', engine='pyarrow', compression='GZIP')


# In[131]:


get_ipython().system('ls /tmp')

