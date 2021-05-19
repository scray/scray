#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import numpy as np
import pandas as pd


# In[1]:


def getMissingSchemaids(pf):
    a = np.array([])
    for hash in hashes:  
        if len(pf[pf['hashvalue'] == hash]) != len(pd.unique(pf[pf['hashvalue'] == hash]['timestamp'])):
            value = pd.unique(pf[pf['hashvalue'] == hash]['schemaid'])
            if (value in a) == False:
                a = np.append(a,value)
    return a.astype(int)

def usedcolumns(tb):
    col = []
    for column in tb.columns:
        if tb.iloc[0][column] != None :
            col.append(column)
    return col

def getVariableUniqueColums(dataall):
    col = []
    for column in dataall.columns:
        size = len(pd.unique(dataall[column]))
        #print(column,size)
        if size > 1:
            col.append(column)
    return col

def keepcolumns(tb,keep):
    for column in tb.columns:
        if column not in keep :
            del(tb[column])


# In[ ]:


#def getInfoForIgroup(schemaid) :
    
def getInfoForSchema(pfall,schemaid) :
    filteredrows = pfall[pfall['schemaid'] == schemaid]
    return len(pd.unique(filteredrows['__name__'])),            len(pd.unique(filteredrows['hashvalue'])),            len(pd.unique(filteredrows['igroup'])),            len(pd.unique(filteredrows['inode'])),            #getEncoder('__name__').inverse_transform(pd.unique(filteredrows['__name__']))

def getInfoForSchemas(pfall) :
    for schemaid in schemas:
        info = getInfoForSchema(pfall,schemaid)
        if info[3] > 1:
            print(schemaid , info)

