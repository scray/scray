#!/usr/bin/env python
# coding: utf-8

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


# In[4]:


def astype(pfall,selected,newtype):
    for each in selected:
        pfall[each] = pfall[each].astype(newtype)     


# # B

# In[5]:


selected = [ 'CSTATUS', 'CSERVICE',        'CSENDERPROTOCOL', 'CSENDERENDPOINTID',        'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID']


# In[6]:


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

# In[7]:


columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE',       'CSLABILLINGMONTH', 'CSENDERPROTOCOL', 'CSENDERENDPOINTID',       'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT',       'CMESSAGETAT2', 'CSLADELIVERYTIME']
# withot 'CSLABILLINGMONTH'
def get_columns_2():
    columns = ['CGLOBALMESSAGEID', 'CSTARTTIME', 'CENDTIME', 'CSTATUS', 'CSERVICE',            'CSENDERPROTOCOL', 'CSENDERENDPOINTID',           'CINBOUNDSIZE', 'CRECEIVERPROTOCOL', 'CRECEIVERENDPOINTID', 'CSLATAT',           'CMESSAGETAT2', 'CSLADELIVERYTIME']
    return columns
columns = get_columns_2()
#to count messages sent
#columns = [ 'CSTARTTIME', 'CSENDERENDPOINTID']


# In[8]:


sparkSession = dfBasics.getSparkSession()


# In[ ]:


#len(pfall), len(ac)
#pfall.head()
#pfall.dtypes
#len(mdcountsall)


# In[ ]:


#pfall[pfall['CGLOBALMESSAGEID'] == mdcountsall.index.get_level_values(0)[0]]
#pfall['CGLOBALMESSAGEID'] == -9209718302575659389


# In[ ]:


#len(pfall[pfall['CSLADELIVERYTIME'] == -1])
#mdcountsall.index.get_level_values(0)[0]


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


# In[ ]:


#pfall.iloc[g[1][0]]['CENDTIME'],pfall.iloc[g[1][1]]['CENDTIME']


# In[19]:


#len(f), len(mdcountsall),f
#line= '1580137124017'

#from py4j.java_gateway import Py4JJavaError


# In[9]:


# read timestamps from file
with open("/tmp/slatimestamps.txt", "r") as file:
    lines = file.read().split('\n')


# In[ ]:


def getencodedpfall(line) :
    try:
        df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/sla_sql_data/' + line +  '/*').select(columns).dropDuplicates()
        #print('df.toPandas')
        pfall = df.toPandas() 
        #print('1')
        astype(pfall,['CSTARTTIME','CENDTIME','CSLATAT','CMESSAGETAT2','CSLADELIVERYTIME','CINBOUNDSIZE'] ,int) 
        #del(pfall['CSLABILLINGMONTH'])
        pfall['CGLOBALMESSAGEID'] = pfall['CGLOBALMESSAGEID'].apply(hash)
        #print('1b')
        #ac = pd.unique(pfall['CGLOBALMESSAGEID'])
        mdcountsall = pfall.groupby(['CGLOBALMESSAGEID','CSTATUS'])['CSTARTTIME'].count()
        mdcountsall = mdcountsall[mdcountsall > 1]
        f = printtt(pfall,mdcountsall)
        pfall = pfall.drop(f)
        astype(pfall,selected,str) 
        encoder.encode(pfall,selected)
        pfall.to_parquet('/tmp/sla_' + line + '.parquet', engine='fastparquet', compression='GZIP')
    except Exception as e:
        print("does not exist:" + line)


# In[ ]:


#line = lines[0]
#pfall = getencodedpfall(line)


# In[ ]:


for line in lines:
    getencodedpfall(line)
    

