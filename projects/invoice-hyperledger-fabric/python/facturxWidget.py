#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
pd.set_option('display.max_rows', 500)
invoices = pd.read_parquet('./invoices_facturx.parquet')
invoices = invoices['supplyChainTradeTransaction']


# In[4]:


#import numpy.ndarray
import numpy as np

def printall(m,mkey='',space='->'):
    for key, value in m.items():
        #print (type(value))
        if not isinstance(value, type(None)):
            #print(space + mkey, ' {0}: {1}'.format(key, value))
            if isinstance(value, dict):
                #print(space + (str(type(value))) + str(value.keys()))
                printall(value, mkey + "/" + key, space + '->')
            else:
                if isinstance(value,np.ndarray):
                    print('ARRAY',len(value), value)
                print(space + mkey + '/{0}: {1}'.format(key, value) + '(' + str(type(value)) + ')') 
 
def printpaths(m,mkey='',space='->'):
    for key, value in m.items():
        #print (type(value))
        if not isinstance(value, type(None)):
            #print(space + mkey, ' {0}: {1}'.format(key, value))
            if isinstance(value, dict):
                print(key,value)
                #print(key + space + (str(type(value))) + str(value.keys()))
                #printall(value, mkey + "/" + key, space + '->')


def printNotNone(val):
    if isinstance(val, dict):
        for key, value in val.items():
            if not isinstance(value, type(None)):
                print('{0}: {1}'.format(key, value)) 
    else:
        print(val)

def getDictNotNone(val):
    #print('getDictNotNone',type(val))
    if isinstance(val, dict):
        r = dict()
        #print('getDictNotNone',r.keys())
        for key, value in val.items():
            #print('getDictNotNone',key,value)
            if not isinstance(value, type(None)):
                r[key]=value
        return r       
    else:
        return dict        

def getPathValue(m,path=''):
    splits = path[1:].split("/", 1)
    value=m.get(splits[0])
    #print(splits,value)
    if not isinstance(value, type(None)):
        if len(splits) > 1:
            return getPathValue(value,path='/' + splits[1])
    return value            
  
def getSimpleElementValue(d,key):
    __name__='getSimpleElementValue'
    #print(__name__,key,d)
    value=d[key]
    #print(value)
    #print(__name__,key,value, type(value))
    return key,value   
    
def getElementValue(d,key):
    #print(d)
    value=d[key]
    #print(value)
    #print(key,value, type(value))
    if isinstance(value, dict):
        if 'value' in value:
            return key,value['value']
        else: 
            #print("getElementValue",key)
            #print("getElementValue",value.keys())
            #print("getElementValue",value)
            #return key,getElementValue(value,list(value.keys())[0])
            return key,value
    elif isinstance(value,np.ndarray):
        #print(len(value),type(value[0]))
        if 'value' in value[0]:
            return value[0]['value']
        else:
            #print(value[0])
            return getElementValue(value[0],list(value[0].keys())[0])
    elif not isinstance(value, type(None)):
        return key,str(value)
    else:
        return None
            
    
def getElementValue1(d,key):
    #print(d)
    value=d[key]
    #print(value)
    #print(key,value, type(value))
    if isinstance(value, dict):
        if 'value' in value:
            return value['value']
        else: 
            print(key)
            print(value.keys())
            print(value)
            return getElementValue(value,list(value.keys())[0])
    elif isinstance(value,np.ndarray):
        #print(len(value),type(value[0]))
        if 'value' in value[0]:
            return value[0]['value']
        else:
            #print(value[0])
            return getElementValue(value[0],list(value[0].keys())[0])
    elif not isinstance(value, type(None)):
        return str(value)
    else:
        return None
        
def freeze(d):
    if isinstance(d, dict):
        return frozenset((key, freeze(value)) for key, value in d.items())
    elif isinstance(d, list):
        return tuple(freeze(value) for value in d)
    return d            


# In[5]:




def doit(m,path,hideUnused=False):
    __name__='doit'
    print(__name__,type(m))
    r = getPathValue(m, path)
    if isinstance(r, type(None)):
        return
    if hideUnused == True:
        r = getDictNotNone(r)
    print(r)    
    for key in r.keys():
        value=getElementValue(r,key)
        #print (key,value)
        text = widgets.Text(description = key,value = value, style=style, layout=layout)
        display(text)



def doit1(m,path,hideUnused=False):
    __name__='doit1'
    r = getPathValue(m, path)
    #print(__name__,type(r),r,hideUnused)
    if hideUnused == True:
        r = getDictNotNone(r)
    proc1(r,hideUnused)
       

    

## replace by doitElement ???????    
def addChildsElement(r,hideUnused=False): 
    __name__='addChildsElement'
    #print(__name__,r,type(r),hideUnused)
    if isinstance(r, dict):
        doitElement(r,hideUnused=hideUnused)
        for key, value in r.items():
            #print (type(value))
            addHeader(str(key))    
            if not isinstance(value, type(None)):
                #print(space + mkey, ' {0}: {1}'.format(key, value))
                if isinstance(value, dict):
                    #print("addChildsElement",hideUnused,value)
                    doitElement(r,hideUnused=hideUnused)
    else:
        print(__name__,type(r),'not implemented')
    


        

        


# In[6]:


import ipywidgets as widgets
from IPython.display import clear_output
from IPython.display import Javascript, display
import uuid

style = {'description_width': '250px'}
layout = {'width': '500px'}

class facturx_widgets():
    #layout = widgets.Layout(width='auto', height='40px') #set width and height
    #style = {'description_width': '150px'}
    #layout = {'width': '400px'}
    
    def addHeader(self,text='',bold=True):
        if bold == True:
            html = widgets.HTML(
            value="<b>" + text + "</b>",
            description=' ',
            style=style, layout=layout
            )
        else:
            html = widgets.HTML(
            value=text,
            description=' ',
            style=style, layout=layout
            )
        display(html)    

    def addVisText(self,key='',value=''):
        text = widgets.Text(description = key,value = value, style=style, layout=layout)
        display(text)     
    
    def addVisCheckbox(self,key='',value=False):
        text = widgets.Checkbox(description = key,value = value, style=style, layout=layout)
        display(text)   
        
    def procArray(self,r,hideUnused=False,rkey=''):   
        __name__='procArray'
        #print (__name__,type(r))
        if not isinstance(r, np.ndarray):
            print(__name__,type(r),'not implemented')
            return
        for element in r:
            #print (__name__,type(element),element)
            self.doitElement(element,rkey=rkey,hideUnused=hideUnused)
    def procList(self,r,hideUnused=False,rkey=''):   
        __name__='procList' 
        for element in r:
            #print (__name__,type(element),element)
            self.doitElement(element,rkey=rkey,hideUnused=hideUnused)
    # proc dic 
    def proc1(self,r,hideUnused=False,rkey=''):
        __name__='proc1'
        if not isinstance(r, dict):
            print(__name__,type(r),'not implemented')
            return
        #print(__name__,'rkey=' + rkey,r.keys())
    
        if hideUnused == True:
            r = getDictNotNone(r)
                
        ## option: remove value !!!
        if not 'value' in r:
            #print(__name__,'!!!',r)
            self.addHeader(rkey,bold=False)
        #print(__name__,'!!!',r.keys())
        for key in r.keys():
            #value=getElementValue(r,key)
            kk,value=getSimpleElementValue(r,key)
            if not isinstance(value, type(None)):
                #print (__name__,key,type(value),len(value),value, value[1])
                #print (__name__,key,type(value),value)
                #print (__name__,kk,rkey,type(value))
                if isinstance(value, dict):
                    #print(__name__,key,value.keys(),hideUnused)
                    if not 'value' in value:
                        #print(__name__,'222 value',r)
                        self.addHeader(str(key) )
                    #print(__name__,'222',r.keys())
                
                    for kkey in value.keys():
                        key1,value1=getSimpleElementValue(value,kkey)
                    
                        #print(__name__,rkey)
                        #if isinstance(value1, dict):
                        #addHeader(rkey,bold=False)
                        #value1=getElementValue(value[1],kkey)
                        #print (__name__,key1,type(value1))
                        #print (__name__,kkey,value1,type(value1))
                        #print (__name__,kkey,key1,rkey)
                    
                        self.doitElement(value1,rkey=key1,hideUnused=hideUnused,upkey=kk)
                elif isinstance(value, str):
                    if key == 'value' and len(rkey) > 0:
                        key=rkey
                    self.addVisText(key=key,value=value) 
                elif isinstance(value, int):
                    #print(__name__,key,value)
                    if key == 'value' and len(rkey) > 0:
                        key=rkey
                    self.addVisText(key=key,value=str(value))     
                elif isinstance(value,list):    
                    #print(__name__,key,value)
                    self.procList(value,hideUnused=hideUnused,rkey=key)
                elif isinstance(value,np.ndarray):    
                    self.procArray(value,hideUnused=hideUnused,rkey=key)     
                else:
                    print(__name__,'not implemented',type(value))
                value = str(value)
            else:
                self.addVisText(key=key,value=value)  
            #addVisText(key=key,value=value)  
    
    def doitElement(self,r,rkey='',hideUnused=False,upkey=''):
        __name__='doitElement'
        #print(__name__,hideUnused,type(r),rkey,'upkey',upkey)
        #if isinstance(r, type(None)): 
        #    print("doitElement",type(r),hideUnused)
        #    return 
        if isinstance(r, tuple):
            #print(__name__,r,type(r[1]))
            if isinstance(r[1], str):
                #addHeader(rkey)
                self.addVisText(key=r[0],value=r[1])
            else:
                #rkey=rkey + '.'+ r[0]
                #print(__name__,rkey)
                self.addHeader(rkey + __name__,bold=False)
                self.doitElement(r[1],rkey=rkey,hideUnused=hideUnused)
        elif isinstance(r, str):    
            if(rkey == 'value'):
                #print(__name__,rkey,upkey)
                self.addVisText(key=upkey,value=r)
                return
            self.addVisText(key=rkey,value=r)         
        elif isinstance(r, type(None)): 
            #print("doitElement",rkey,r)
            if hideUnused==False: 
                self.addVisText(key=rkey,value=r)
            return
        elif isinstance(r, dict):
            #print(__name__,type(r),r)
            self.proc1(r,rkey=rkey,hideUnused=hideUnused)    
        elif isinstance(r,np.ndarray):    
            #print(__name__,type(r),r)
            self.procArray(r,hideUnused=hideUnused,rkey=rkey) 
        elif isinstance(r, bool):  
            self.addVisCheckbox(key=rkey,value=r)
            #addVisText(key=rkey,value=str(r)) 
        elif isinstance(r, int):  
            self.addVisText(key=rkey,value=str(r))     
        else:
            print(__name__,type(r),'not implemented')

    def addChilds(self,m,path,hideUnused=False):
        __name__='addChilds'
        #print(__name__,hideUnused)
        r = getPathValue(m, path)
        #print(__name__,hideUnused,type(r))
        self.doitElement(r,hideUnused=hideUnused)
        return    
         
    def __init__(self,m,path,hideUnused=False): 
        self.addChilds(m,path,hideUnused=hideUnused)

