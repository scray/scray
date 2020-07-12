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
    return dft.sort_values('count')

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

def printtt():    
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
    ac = pd.unique(pfall['CGLOBALMESSAGEID'])
    tt = getCountDF(pfall,'CGLOBALMESSAGEID',ac)
    
    col = printtt()   
    pfall = pfall.drop(col)
    
    astype(pfall,selected,str) 
    encoder.encode(pfall,selected)
    astype(pfall,['CSTARTTIME','CENDTIME','CSLATAT','CMESSAGETAT2','CSLADELIVERYTIME','CINBOUNDSIZE'] ,int) 
    #del(pfall['CSLABILLINGMONTH'])
    pfall['CGLOBALMESSAGEID'] = pfall['CGLOBALMESSAGEID'].apply(hash)
    pfall = pfall.drop_duplicates()
    
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


# In[ ]:





# In[8]:


sparkSession = dfBasics.getSparkSession()


# In[ ]:





# In[9]:


# read timestamps from file
with open("/tmp/slatimestamps.txt", "r") as file:
    lines = file.read().split('\n')


# In[ ]:


for line in lines:
    df = sparkSession.read.parquet('hdfs://172.30.17.145:8020/sla_sql_data/' + line +  '*/*').select(columns).dropDuplicates()
    pfall = df.toPandas() 
    encodepfall(pfall,line)

