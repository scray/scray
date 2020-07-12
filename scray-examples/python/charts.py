#!/usr/bin/env python
# coding: utf-8

# In[ ]:


## Setup charts
import pandas as pd
pd.plotting.register_matplotlib_converters()
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
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


def createLineplot(md,fx,fy,fontscale,title="",skip=0,rot=90) :
    sns.set(style='whitegrid', palette='muted', font_scale=fontscale)
    plt.figure(figsize=(fx,fy))
    plt.title(title)
    ax = sns.lineplot(x=md['date'], y=md['value'], data=md)
    #ax = sns.lineplot(data=md)
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

