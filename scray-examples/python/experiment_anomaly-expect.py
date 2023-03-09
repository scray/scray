#!/usr/bin/env python
# coding: utf-8

# # init

# In[1]:


# demo: loads file with all messages (CSTARTTIME, CSENDERENDPOINTID, ymdhm )
# show some charts, anomaly detection with LSTM autoencoders


# In[2]:


import base.dfBasics as dfBasics
import base.common as common
import base.encoder as encoder
import base.pfAdapt as pfAdapt
#import base.charts as charts
#import base.anomaly as anomaly

import pandas as pd    
from pyspark.sql import functions


# In[3]:


sparkSession = dfBasics.getSparkSession()


# ### load data

# In[4]:


print('load data')
df = pd.read_parquet('/home/jovyan/work/output/sla_enc_all_5.parquet')
print('load data 1')


# In[5]:


senders = pd.unique(df['CSENDERENDPOINTID'])


# In[6]:


sender = senders[0]
pfall = df[df['CSENDERENDPOINTID'] == sender]


# In[7]:


#pfall.head()
print('load data 2')


# ## functions

# In[8]:


## Setup charts
import pandas as pd
pd.plotting.register_matplotlib_converters()
import matplotlib.pyplot as plt
# %matplotlib inline
import seaborn as sns
print("Setup Complete")

def get_ym_string(a,b) :
    return a + "-" + b
    #return a.join(["-",b]) 

def get_ym(mdcountsall) :
    a = mdcountsall.index.get_level_values(0).astype(str)
    b = mdcountsall.index.get_level_values(1).astype(str)
    return a,b

def get_ymd(mdcountsall) :
    a = mdcountsall.index.get_level_values(0).astype(str)
    b = mdcountsall.index.get_level_values(1).astype(str)
    c = mdcountsall.index.get_level_values(2).astype(str)
    return a,b,c

def make_2digits(blist):
    for n, b in enumerate(blist):
        if int(b) < 10:
             blist[n] = '0' + str(b)
    return blist

def get_ymd_string(a,b,c) :
    if isinstance(a, str) :
        return a + "-" + make_2digits([b])[0] + "-" + make_2digits([c])[0] 
    elif isinstance(a,pd.core.series.Series):
        return a.astype(str) + "-" + make_2digits(b.astype(str)) + "-" + make_2digits(c.astype(str))
    return a + "-" + pd.Index(make_2digits(b.tolist())) + "-" + pd.Index(make_2digits(c.tolist())) 

def get_ymdh(mdcountsall) :
    a = mdcountsall.index.get_level_values(0).astype(str)
    b = mdcountsall.index.get_level_values(1).astype(str)
    c = mdcountsall.index.get_level_values(2).astype(str)
    d = mdcountsall.index.get_level_values(3).astype(str)
    return a,b,c,d

def get_ymdhm(mdcountsall) :
    a = mdcountsall.index.get_level_values(0).astype(str)
    b = mdcountsall.index.get_level_values(1).astype(str)
    c = mdcountsall.index.get_level_values(2).astype(str)
    d = mdcountsall.index.get_level_values(3).astype(str)
    e = mdcountsall.index.get_level_values(4).astype(str)
    return a,b,c,d,e

def get_ymdh_string(a,b,c,d) :
    if isinstance(a, str) :
        return a + "-" + make_2digits([b])[0] + "-" + make_2digits([c])[0] + "-" + make_2digits([d])[0]
    elif isinstance(a,pd.core.series.Series):
        return a.astype(str) + "-" + make_2digits(b.astype(str)) + "-" + make_2digits(c.astype(str)) + "-" + make_2digits(d.astype(str))
    return a + "-" + pd.Index(make_2digits(b.tolist())) + "-" + pd.Index(make_2digits(c.tolist())) + "-" + pd.Index(make_2digits(d.tolist()))

def get_ymdhm_string(a,b,c,d,e) :
    if isinstance(a, str) :
        return a + "-" + make_2digits([b])[0] + "-" + make_2digits([c])[0] + "-" + make_2digits([d])[0] + "-" + make_2digits([e])[0]
    elif isinstance(a,pd.core.series.Series):
        return a.astype(str) + "-" + make_2digits(b.astype(str)) + "-" + make_2digits(c.astype(str)) + "-" + make_2digits(d.astype(str)) + "-" + make_2digits(e.astype(str))
    return a + "-" + pd.Index(make_2digits(b.tolist())) + "-" + pd.Index(make_2digits(c.tolist())) + "-" + pd.Index(make_2digits(d.tolist())) + "-" + pd.Index(make_2digits(e.tolist()))


def createData_ym(pfall,month) :
    if (month > 0) & (month < 13) :
        mdcountsall = pfall[(pfall['month'] == month)].groupby(['year','month'])['year'].count()
    else :
        mdcountsall = pfall.groupby(['year','month'])['year'].count()    
    a,b = get_ym(mdcountsall)
    data2 = pd.DataFrame()
    data2['date'] = get_ym_string(a,b)
    data2['outcome'] = mdcountsall.reset_index(level=0, drop=True).reset_index()['year'].astype(int) 
    return data2

def createData(pfall,month=-1,year=2020,outcome='outcome') :
    if (month > 0) & (month < 13) :
        mdcountsall = pfall[(pfall['year'] == year) & (pfall['month'] == month)].groupby(['year','month','day','hour'])['year'].count()
    else :
        mdcountsall = pfall.groupby(['year','month','day','hour'])['year'].count()    
    a,b,c,d = get_ymdh(mdcountsall)
    data2 = pd.DataFrame()
    data2['date'] = get_ymdh_string(a,b,c,d)
    data2[outcome] =  mdcountsall.reset_index(level=0, drop=True).reset_index()['year'].astype(int)

    #for pivot table
    data2['hours'] =  d.astype(int) 
    data2['days']  =  c.astype(int) 
    return data2


def createData_ymd(pfall,month,year=2020) :
    if (month > 0) & (month < 13) :
        mdcountsall = pfall[(pfall['year'] == year) & (pfall['month'] == month)].groupby(['year','month','day'])['year'].count()
    else :
        mdcountsall = pfall.groupby(['year','month','day'])['year'].count()    
    a,b,c = get_ymd(mdcountsall)
    data2 = pd.DataFrame()
    data2['date'] = get_ymd_string(a,b,c)
    data2['year'] = a.astype(int) 
    data2['month'] = b.astype(int) 
    data2['day'] = c.astype(int) 
    data2['outcome'] = mdcountsall.reset_index(level=0, drop=True).reset_index()['year'].astype(int) 
    return data2

def createData_ymdh(pfall,month,year=2020) :
    if (month > 0) & (month < 13) :
        mdcountsall = pfall[(pfall['year'] == year) & (pfall['month'] == month)].groupby(['year','month','day','hour'])['year'].count()
    else :
        mdcountsall = pfall.groupby(['year','month','day','hour'])['year'].count()    
    a,b,c,d = get_ymdh(mdcountsall)
    data2 = pd.DataFrame()
    data2['date'] = get_ymdh_string(a,b,c,d)
    data2['year'] = a.astype(int) 
    data2['month'] = b.astype(int) 
    data2['day'] = c.astype(int) 
    data2['hour'] = d.astype(int)
    data2['outcome'] = mdcountsall.reset_index(level=0, drop=True).reset_index()['year'].astype(int) 
    return data2

def createData_ymdhm(pfall,month,year=2020) :
    if (month > 0) & (month < 13) :
        mdcountsall = pfall[(pfall['year'] == year) & (pfall['month'] == month)].groupby(['year','month','day','hour','minute'])['year'].count()
    else :
        mdcountsall = pfall.groupby(['year','month','day','hour','minute'])['year'].count()    
    a,b,c,d,e = get_ymdhm(mdcountsall)
    data2 = pd.DataFrame()
    data2['date'] = get_ymdh_string(a,b,c,d)
    data2['year'] = a.astype(int) 
    data2['month'] = b.astype(int) 
    data2['day'] = c.astype(int) 
    data2['hour'] = d.astype(int)
    data2['minute'] = e.astype(int)
    data2['outcome'] = mdcountsall.reset_index(level=0, drop=True).reset_index()['year'].astype(int) 
    return data2


def createData_column_ymdh(pfall,month=-1,year=2020, column=None) :
    if (month > 0) & (month < 13) :
        mdcountsall = pfall[(pfall['year'] == year) & (pfall['month'] == month)]
    else :
        mdcountsall = pfall 
    #a,b,c,d = get_ymdh(mdcountsall)
    data2 = pd.DataFrame()
    data2['date'] = get_ymdh_string(pfall['year'], pfall['month'], pfall['day'],pfall['hour'])
    df2 = mdcountsall[['year', 'month', 'day','hour',column]].copy()
    data2 = pd.concat([data2, df2], axis=1)
    data2.columns = list(data2.columns[:-1]) + ['outcome']
    
    return data2


def createData_column_ymd(pfall,month=-1,year=2020, column=None) :
    if (month > 0) & (month < 13) :
        mdcountsall = pfall[(pfall['year'] == year) & (pfall['month'] == month)]
    else :
        mdcountsall = pfall 
    #a,b,c,d = get_ymdh(mdcountsall)
    data2 = pd.DataFrame()
    data2['date'] = get_ymd_string(pfall['year'], pfall['month'], pfall['day'])
    df2 = mdcountsall[['year', 'month', 'day',column]].copy()
    data2 = pd.concat([data2, df2], axis=1)
    data2.columns = list(data2.columns[:-1]) + ['outcome']
    
    return data2


def label(graph,skip,rot) :
    #print(len(graph.get_xticklabels()))
    for ind, label in enumerate(graph.get_xticklabels()):
        if ind % skip == 0:  # every 10th label is kept
            label.set_visible(True)
            label.set_rotation(rot)
        else:
            label.set_visible(False)


# In[9]:


def label_skip(a):
    print(a)
    b=a
    if a > 12:
        b = (a - 12) / 12
    return int(b)    


# ### visualization

# In[10]:


def createBarplot(md=None,fx=24,fy=12,fontscale=3.0,title="") :
    sns.set(style='whitegrid', palette='muted', font_scale=fontscale)
    plt.figure(figsize=(fx,fy))
    plt.title(title)
    ax = sns.barplot(x=md['date'], y=md['outcome'], data=md)
    label(ax,label_skip(len(ax.get_xticklabels())),75)
    plt.setp( ax.xaxis.get_majorticklabels(), rotation=75 )
    plt.tight_layout()
    plt.show()
    return ax

## heatmap
def createHeatmap(piv,title="") :
    plt.figure(figsize=(24,8))
    plt.title(title)
    ax = sns.heatmap(piv, square=True)
    plt.setp( ax.xaxis.get_majorticklabels(), rotation=0 )
    plt.tight_layout()
    plt.show()
    return ax


# In[11]:


#pfall
#pfall5 = createData_column_ymdh(pfall,column='CMESSAGETAT2')
#get_ymdh_string(pfall5['year'], pfall5['month'], pfall5['day'],pfall5['hour'])
#pfall5['year']
#pfall5['year'].astype(str) + '_' + make_2digits(pfall5['month'].astype(str))
#make_2digits([c])[0] 

#pfall5.columns = pfall5.columns[:-1] + 'outcome'
#pfall5.columns = list(pfall5.columns[:-1]) + ['outcome']
#pfall5


# In[12]:


#year=2022
#month=7
#mdcountsall = pfall[(pfall['year'] == year) & (pfall['month'] == month)].groupby(['year','month','day','hour'])['year'].count()


# In[13]:


#pfall


# In[14]:


#checka(pfall[(pfall[year]== 2022) & (pfall[month]== 7)])
#pfall[(pfall[year]== '2022') & (pfall[month]== '7')]
#pfall[(pfall[year] == 2022)] 
#pfall
#pfall5 = createData_column_ymdh(pfall,column='CMESSAGETAT2')
#pfall6 = checka(pfall5)
#pd.unique(pfall5['year'])


# ### check

# In[ ]:





# In[15]:


from calendar import monthrange

def get_month(pda, year=2020, month=1):
    return pda[(pda['month'] == month) & (pda['year'] == year)]

def is_complete(pda, year=2020, month=1):
    mm = pda[(pda['month'] == month) & (pda['year'] == year)]
    if len(mm) > 0:
        return monthrange(year, month)[1] == len(mm)
    return None
    
def check_complete(pda, year=2020):
    for m in range(1, 13):
        mm = pda[(pda['month'] == m) & (pda['year'] == year)]
        if len(mm) > 0:
            print(m,monthrange(year, m)[1] == len(mm))

            
def add_line_to_dataframe(df=None,year=None,month=None,day=None,hour=None,minute=None,value=0): 
    
    if hour is not None:
        if minute is not None:
            _date = get_ymdhm_string(str(year),str(month),str(day),str(hour),str(minute)) 
            df.loc[len(df)]=[_date,year,month,day,hour,minute,value]  
        else:    
            _date = get_ymdh_string(str(year),str(month),str(day),str(hour)) 
            df.loc[len(df)]=[_date,year,month,day,hour,value]   
    else:    
        _date = get_ymd_string(str(year),str(month),str(day))
        df.loc[len(df)]=[_date,year,month,day,value]     
    
def check_and_fill_hours_of_day(df=None,year=2022,month=7,day=None):
    _df=df[(df['year']==year) & (df['month']==month) & (df['day']==day)] 
    #print(_df)
    
    minutes = False
    if 'minute' in df.columns:
        minutes = True
    
    for hour in range(0, 24):
        if len(_df[_df['hour']==hour]) == 0:
            if minutes == True:
                for minute in range(0, 59):
                    #_date = get_ymdhm_string(str(year),str(month),str(day),str(hour),str(minute))  
                    add_line_to_dataframe(df=df,year=year,month=month,day=day,hour=hour,minute=minute) 
            else:
                #_date = get_ymdh_string(str(year),str(month),str(day),str(hour))  
                add_line_to_dataframe(df=df,year=year,month=month,day=day,hour=hour) 
        else:
            if minutes == True:
                _df2 = _df[_df['hour']==hour]
                for minute in range(0, 59):
                    if len(_df2[_df2['minute']==minute]) == 0:
                        add_line_to_dataframe(df=df,year=year,month=month,day=day,hour=hour,minute=minute)
            else:
                pass
    #print (df)        
    return df     
    
    
#pda :  date,year,month,day.outcome 
def fill(pda,year,month,min_day=1,max_day=-1,hours=False): 
    if max_day == -1:
        days = monthrange(year, month)[1]
    else:
        days = max_day
        
    #print(min_day,days,max_day,hours)     
    minutes = False
    if 'minute' in pda.columns:
        minutes = True
        
    df = pda[(pda['month'] == month) & (pda['year'] == year)]
    for day in range(min_day, days + 1):
        if len(df[df['day']==day]) == 0:
            #print(day)
            if hours == True:
                for hour in range (0,24):
                    if minutes == True:
                        for minute in range(0, 59): 
                            add_line_to_dataframe(df=pda,year=year,month=month,day=day,hour=hour,minute=minute) 
                    else:
                        add_line_to_dataframe(df=pda,year=year,month=month,day=day,hour=hour) 
            else:    
                add_line_to_dataframe(df=pda,year=year,month=month,day=day)
        else:
            if hours == True:
                #print(year,month,day)
                check_and_fill_hours_of_day(df=pda,year=year,month=month,day=day)
       
    
def check(pda):
    _start_year = min(pd.unique(pda['year']))
    _end_year = max(pd.unique(pda['year']))  
    _start_month = min(pd.unique(pda[pda['year']== _start_year]['month']))
    _end_month = max(pd.unique(pda[pda['year']== _end_year]['month']))
    _end = 13
    
    if 'hour' in pda.columns:
        hours = True
    else:
        hours = False
        
    for year in range(_start_year,_end_year+1):   
        for month in range(_start_month,_end):        
            #print(year,month, _end_month, _end_year)
            
            if((month == _start_month) & (year == _start_year)):
                min_day = (min(list(pda[(pda['month'] == month) & (pda['year'] == year)]['day'])))
            else:
                min_day = 1
                
            if((month == _end_month) & (year == _end_year)):
                max_day = (max(list(pda[(pda['month'] == month) & (pda['year'] == year)]['day'])))
                #print(month,year,max_day)
                fill(pda,year,month,min_day=min_day, max_day=max_day,hours=hours)
            else:    
                fill(pda,year,month,min_day=min_day,hours=hours)
            if (year == _end_year) &  (month == _end_month):
                #print('DONE')
                break    
        _start_month=1
            
        if year == (_end_year-1):
            _end = _end_month + 1     
            
        if (year == _end_year) &  (month == _end_month):
            #print('DONE')
            break       
            


# ### checka

# In[16]:


# 2019-10-29-00
# 2022-10-06-23

def mean_column(column):
    return int(round(column.mean(),0))

def check_and_fill_hours_of_daya(df=None,year=2022,month=7,day=None, outcome='outcome'):
    _df=df[(df['year']==year) & (df['month']==month) & (df['day']==day)] 
    
    df=pd.DataFrame(columns=['date', 'year', 'month', 'day', 'hour', outcome])
    for hour in range(0, 24):
        _hours = _df[_df['hour']==hour]
        if len(_df[_df['hour']==hour]) == 0:
            _date = get_ymdh_string(str(year),str(month),str(day),str(hour))  
            add_line_to_dataframe(df=df,year=year,month=month,day=day,hour=hour) 
            pass
        else:
            add_line_to_dataframe(df=df,year=year,month=month,day=day,hour=hour,value=mean_column(_hours[outcome]))
    #print (df)      
    return df 

#pda :  date,year,month,day.outcome 
def filla(pda,year,month,min_day=1,max_day=-1,hours=False, outcome='outcome'): 
    if max_day == -1:
        days = monthrange(year, month)[1]
    else:
        days = max_day
        
    #df = pda[(pda['month'] == month) & (pda['year'] == year)]
    if hours == True:
        df=pd.DataFrame(columns=['date', 'year', 'month', 'day', 'hour', outcome])
    else:
        df=pd.DataFrame(columns=['date', 'year', 'month', 'day', outcome])
        
    for day in range(min_day, days + 1):
        if hours == True:
            df_b = check_and_fill_hours_of_daya(df=pda,year=year,month=month,day=day,outcome=outcome)
            df = pd.concat([df, df_b], ignore_index=True)   
        else:
            _df=pda[(pda['year']==year) & (pda['month']==month) & (pda['day']==day)]
            if len(_df) > 0 :
                #print(len(_df),mean_column(_df['outcome']), list(_df['outcome']))
                value=mean_column(_df[outcome])
            else:
                value=0
            add_line_to_dataframe(df=df,year=year,month=month,day=day,value=value) 
    return df   
            
    
def checka(pda, outcome='outcome'):
    _start_year = min(pd.unique(pda['year']))
    _end_year = max(pd.unique(pda['year']))  
    _start_month = min(pd.unique(pda[pda['year']== _start_year]['month']))
    _end_month = max(pd.unique(pda[pda['year']== _end_year]['month']))
    _end = 13
    
    if 'hour' in pda.columns:
        hours = True
        df=pd.DataFrame(columns=['date', 'year', 'month', 'day', 'hour', outcome])
    else:
        hours = False
        df=pd.DataFrame(columns=['date', 'year', 'month', 'day', outcome])
    
    
    for year in range(_start_year,_end_year+1):   
        for month in range(_start_month,_end):        
            #print(year,month, _end_month, _end_year)
            
            if((month == _start_month) & (year == _start_year)):
                min_day = (min(list(pda[(pda['month'] == month) & (pda['year'] == year)]['day'])))
            else:
                min_day = 1
                
            if((month == _end_month) & (year == _end_year)):
                max_day = (max(list(pda[(pda['month'] == month) & (pda['year'] == year)]['day'])))
                #print(month,year,max_day)
                df_b = filla(pda,year,month,min_day=min_day, max_day=max_day,hours=hours,outcome=outcome)
                df = pd.concat([df, df_b], ignore_index=True)
            else:    
                df_b =  filla(pda,year,month,min_day=min_day,hours=hours,outcome=outcome)
                df = pd.concat([df, df_b], ignore_index=True)
            if (year == _end_year) &  (month == _end_month):
                #print('DONE')
                break    
        _start_month=1
            
        if year == (_end_year-1):
            _end = _end_month + 1     
            
        if (year == _end_year) &  (month == _end_month):
            #print('DONE')
            break       
    return df        
           


# In[17]:


def get_date_list(pda, hours=False):
    _start_year = min(pd.unique(pda['year']))
    _end_year = max(pd.unique(pda['year']))  
    _start_month = min(pd.unique(pda[pda['year']== _start_year]['month']))
    _end_month = max(pd.unique(pda[pda['year']== _end_year]['month']))
    _end = 13
    
    _date_list = []
    
    for year in range(_start_year,_end_year+1):   
        for month in range(_start_month,_end):        
            #print(year,month, _end_month, _end_year)
            
            if((month == _start_month) & (year == _start_year)):
                min_day = (min(list(pda[(pda['month'] == month) & (pda['year'] == year)]['day'])))
            else:
                min_day = 1
            
            if((month == _end_month) & (year == _end_year)):
                days = (max(list(pda[(pda['month'] == month) & (pda['year'] == year)]['day'])))
            else:
                days = monthrange(year, month)[1]
            
            for day in range(min_day, days + 1):
                if hours == True:
                    for hour in range(0, 24):
                        _date = get_ymdh_string(str(year),str(month),str(day),str(hour)) 
                        _date_list.append(_date) 
                else:
                    _date = get_ymd_string(str(year),str(month),str(day))
                    _date_list.append(_date)    
            
            if (year == _end_year) &  (month == _end_month):
                #print('DONE')
                break    
        _start_month=1
            
        if year == (_end_year-1):
            _end = _end_month + 1     
            
        if (year == _end_year) &  (month == _end_month):
            #print('DONE')
            break       
    return _date_list        
           


# In[18]:


def check_and_fill_hours_of_month(dataframe=None,year=2022,month=7):
    
    days=pd.unique(dataframe['days'])
    _start_day = min(days)
    _end_day   = max(days)  

    for day in range(_start_day,_end_day+1):
        _df=dataframe[dataframe['days']==day] 
        for hour in range(0, 24):
            if len(_df[_df['hours']==hour]) == 0:
                _date = get_ymdh_string(str(year),str(month),str(day),str(hour))  
                #print([_date,0,hour,day])
                dataframe.loc[len(dataframe)]=[_date,0,hour,day]
    return dataframe       


# In[19]:


def check_complete(dataframe=None,year=2020):
    for m in range(1, 13):
        #print(m)
        mm = dataframe[(dataframe['month'] == m) & (dataframe['year'] == year)]
        print(m,monthrange(year, m)[1] == len(mm))

def fill1(pda,year,month):        
    days = monthrange(year, month)[1]
    df = pda[(pda['month'] == month) & (pda['year'] == year)]
    for day in range(1, days + 1):
        if len(df[df['day']==day]) == 0:
            pda.loc[len(pda)]=[get_ymd_string(str(year),str(month),str(day)),year,month,day,0]
    


# In[20]:


def unique(pfall):
    return pd.unique(pfall['CRECEIVERENDPOINTID']), pd.unique(pfall['CSTATUS']), pd.unique(pfall['CSERVICE']), pd.unique(pfall['CSENDERPROTOCOL']), pd.unique(pfall['CRECEIVERPROTOCOL'])

#un=unique(pfall)
#unique(pfall[pfall['CRECEIVERENDPOINTID']==725])

#pfall0 = pfall[pfall['CRECEIVERENDPOINTID']==un[0][1]]
#pda = createData_ymd(pfall0,0)

#del pda['index'] 


# In[21]:


class Time(object):         
    def __init__(self,year=None,month=None,day=None):
        self.year  = self._int_value(year)
        self.month = self._int_value(month)
        self.day   = self._int_value(day)
    
    def _int_value(self,value):
        if value == '' or value == None:
            return None
        return int(value)
        

class TimeRange(object):    
    def __init__(self,dataframe=None,year_from=None,month_from=None,day_from=None,year_to=None,month_to=None,day_to=None):
        if dataframe is None:
            self.start = Time(year=year_from,month=month_from,day=day_from).__dict__
            self.end   = Time(year_to,month_to,day_to).__dict__
        else:
            #print(dataframe)
            year_from = min(pd.unique(dataframe['year']))
            year_to = max(pd.unique(dataframe['year']))  
            month_from = min(pd.unique(dataframe[dataframe['year']== year_from]['month']))
            month_to = max(pd.unique(dataframe[dataframe['year']== year_to]['month']))
            day_from = min(pd.unique(dataframe[(dataframe['year']== year_from) & (dataframe['month'] == month_from)]['day']))
            day_to = max(pd.unique(dataframe[(dataframe['year']== year_to) & (dataframe['month'] == month_to)]['day']))
            self.start = Time(year_from,month_from,day_from).__dict__
            self.end   = Time(year_to,month_to,day_to).__dict__
            
#TimeRange(dataframe=pfall).__dict__  

def set_date_widget_value(element, value):
    if value != None:
        element.value = str(value)
    else:
        element.value = ''

def init_date_widget(_res):
    _range = TimeRange(dataframe=_res)
    set_date_widget_value(year_from, _range.start['year'])
    set_date_widget_value(month_from, _range.start['month'])
    set_date_widget_value(day_from, _range.start['day'])
    
    set_date_widget_value(year_to, _range.end['year'])
    set_date_widget_value(month_to, _range.end['month'])
    set_date_widget_value(day_to, _range.end['day'])


# In[22]:


def createHeatmapPfall(pfall=None,sender='all',month=1,year=2020,values='outcome',index='hours',columns='days'):
    #print(month,year)
    global data2
    global piv
    data2 = createData(pfall,month,year=year)
    piv = pd.pivot_table(data2, values=values,index=[index], columns=[columns], fill_value=0)
    #titlestring = "CSENDERENDPOINTID: " + str(topsender.iloc[7]['CSENDERENDPOINTID']) + ": "+ category + " so far = " + str(topsender.iloc[7]['outcome']) + " , month: " + str(month) 
    #titlestring = "CSENDERENDPOINTID: " + str(topsender.iloc[7]['CSENDERENDPOINTID']) + ": "+ category  + " month: " + str(month) 
    titlestring ="number messages " + str(year) + "-" + str(month) + "  " + sender
    sns.set(style='whitegrid', palette='muted', font_scale=1.2)
    createHeatmap(piv, titlestring)


# In[23]:


def on_value_submit_month(change):
    #adapt_all()    
    sender=0
   
    with out:
        clear_output()
        month=int(month_from.value)
        year=int(year_from.value)
        createHeatmapPfall(pfall0,"endpoint " + str(CSENDERENDPOINTID) + ' --> ' + str(CRECEIVERENDPOINTID),month=month, year=year)
        #print(int(month_from.value), year_from.value)
        #createHeatmapPfall(pfall=pfall,sender='all',month=1,year=2020)
        
        md2 = createData_ymd(pfall0,month,year=year)
        ax=createBarplot(md2,fx=24,fy=12,fontscale=3.0,title="number messages " + "endpoint " + str(CSENDERENDPOINTID) + ' --> ' + str(CRECEIVERENDPOINTID))
        label(ax,1000,90)


# ### other

# In[24]:


def replace_index_by_date_column(df,column='date'):
    df.set_index(df[column], inplace=True)
    #del df['index']
    #del df['date']
    return df


# ## create data

# In[25]:


def create_data(pfall):
    global pda_hour
    global pda_CMESSAGETAT2_hour
    
    pfall1 = pfall.sort_values(['year','month','day','hour']).reset_index()
    del pfall1['index']
    del pfall1['CGLOBALMESSAGEID']

    pfall0 = pfall

    ####
    pda_hour = createData_ymdh(pfall0,0)
    check(pda_hour)
    pda_hour = pda_hour.sort_values(['date']).reset_index()
    _index = replace_index_by_date_column(pda_hour)

    ####
    #_column = 'CINBOUNDSIZE'
    _column='CMESSAGETAT2'
    pfall5 = createData_column_ymdh(pfall,column=_column)
    pda_CMESSAGETAT2_hour = checka(pfall5 ).sort_values(['date'])
    #pda_CINBOUNDSIZE_hour = checka(pfall5 ).sort_values(['date'])
    _index = replace_index_by_date_column(pda_CMESSAGETAT2_hour)


# # Time Series Anomaly Detection with LSTM Autoencoders (selected sender)

# In[26]:


def get_trained_period(anomalyEnc):
    return anomalyEnc.train.index[0], anomalyEnc.train.index[len(anomalyEnc.train.index)-1]

def get_test_period(anomalyEnc):
    return anomalyEnc.test.index[0], anomalyEnc.test.index[len(anomalyEnc.test) -1]

def get_period(pfall,percent = 1.0):
    max_index = int(len(pfall1) * percent)
    return pfall.iloc[pfall.index[0]]['date'], pfall.iloc[pfall.index[max_index -1]]['date']

def get_percent(pfall1,year=None,month=None,day=None,hour=None):
    _pfall = pfall1[(pfall1['year'] == year) & (pfall1['month'] == month)]
    if day != None:
        _pfall = _pfall[(_pfall['day'] == day)]
    if hour != None:
        _pfall = _pfall[(_pfall['hour'] == hour)]
    
    index = _pfall.index[len(_pfall.index)-1]    
    #print(index)
    index = pfall1.index.get_loc(index)
    
    return (index + 1) / len(pfall1)
    #return _pfall.index
    
def get_index_period(pfall):
    max_index = int(len(pfall) * percent) -1
    return pfall.index[0], pfall.index[max_index]
    


# ### train models

# In[69]:


import AnomalyDetectionLSTMAutoencoder
import datetime
import datetime as dt

def train_model(dataframe=None, time_steps=30, year=2022,month=6,day=None,hour=None):
    anomalyEnc = AnomalyDetectionLSTMAutoencoder.AnomalyDetectionLSTMAutoencoder(TIME_STEPS = time_steps)
    #anomalyEnc.initAndTrain_divide(dataframe, get_percent(dataframe,year=year,month=month,day=day,hour=hour))
    
    dataframe['datetime']  = pd.to_datetime(dataframe[["year", "month", "day", "hour"]])
    
    anomalyEnc.df1 = anomalyEnc.createDataframe(dataframe)
    anomalyEnc.df1['datetime'] = dataframe['datetime']
    
    _day = day
    if _day is None:
        _day = 1    
    date = datetime.datetime(year,month,_day)
    dates = dataframe['datetime']
    _dt = min(dates, key=lambda d: abs(d - date))
    perc_train = get_percent(pda_hour,year=_dt.year,month=_dt.month,day=day,hour=hour)    
    if perc_train == 1.0:
        perc_train = 0.7
        
    #perc_train = get_percent(dataframe,year=year,month=month,day=day,hour=hour)
    train, test = anomalyEnc.getTrainAndTest(anomalyEnc.df1,perc_train)
    anomalyEnc.initAndTrain(train=train, test=test)
    
    return anomalyEnc


# In[28]:


def get_anomalies(anomalyEnc,threshold):
    anomalyEnc.evaluateAnomalies(threshold)
    anomalyEnc.anomalies['datetime'] = anomalyEnc.test['datetime']
    return anomalyEnc.anomalies


# In[29]:


def numpy_to_dataframe(b):
    _df = pd.DataFrame()
    _df['outcome'] = b.reshape([1, len(b)])[0]
    return _df


# In[30]:


def train_models():
    global anomalyEnc_hour
    global anomalyEnc_CMESSAGETAT2_hour
    anomalyEnc_hour = train_model(dataframe=pda_hour,time_steps=24, year=2020,month=12)
    _anomalies = get_anomalies(anomalyEnc_hour,0.9).index

    anomalyEnc_CMESSAGETAT2_hour = train_model(dataframe=pda_CMESSAGETAT2_hour,time_steps=24, year=2020,month=12)
    _anomalies = get_anomalies(anomalyEnc_CMESSAGETAT2_hour,0.9).index


# #### SAVE scaler, test 

# In[31]:


#https://machinelearningmastery.com/how-to-save-and-load-models-and-data-preparation-in-scikit-learn-for-later-use/
#anomalyEnc2.test.head()
#anomalyEnc2.scaler

from pickle import dump
# save the model
#dump(model, open('model.pkl', 'wb'))
# save the scaler

def save_models():
    from pathlib import Path
    _path = '/home/jovyan/work/output/experiment_anomaly_expect/' + str(sender) + '/anomalyEnc_hour/'
    _enc = anomalyEnc_hour
    Path(_path).mkdir(parents=True, exist_ok=True)
    dump(_enc.scaler, open(_path + 'scaler.pkl', 'wb'))
    _enc.test.to_parquet(_path + 'test.parquet')

    _path = '/home/jovyan/work/output/experiment_anomaly_expect/' + str(sender) + '/anomalyEnc_CMESSAGETAT2_hour/'
    _enc = anomalyEnc_CMESSAGETAT2_hour
    Path(_path).mkdir(parents=True, exist_ok=True)
    dump(_enc.scaler, open(_path + 'scaler.pkl', 'wb'))
    _enc.test.to_parquet(_path + 'test.parquet')


# ### functions

# In[32]:


def scatterplot(index, values,label):
    return sns.scatterplot(
      x=index,
      y=values,
      color=sns.color_palette()[3],
      s=152,
      label=label
    )
    
def plot_test(test,scaler,anomalies,titlestring,xlabel):

    fig = plt.figure(figsize=(18,9))
    
    plt.plot(
      test[anomalyEnc.TIME_STEPS:].index, 
      #scaler.inverse_transform(test[TIME_STEPS:].close), 
      scaler.inverse_transform(test[anomalyEnc.TIME_STEPS:]),   
      label='msg count'
    );

    #ax = scatterplot(anomalyEnc.anomalies.index, anomalyEnc.scaler.inverse_transform(anomalyEnc.anomalies['close']), 'anomaly')  
    #ax = scatterplot(anomalyEnc.anomalies.index, anomalyEnc.scaler.inverse_transform(anomalyEnc.anomalies), 'anomaly')
    _a=anomalyEnc.anomalies['close']
    _a = pd.DataFrame(_a)
    _a = anomalyEnc.scaler.inverse_transform(_a)
    _a = _a[:, 0]
    #print(_a,type(_a),_a[0],len(_a),len(_a[:, 0]))
    ax = scatterplot(anomalyEnc.anomalies.index, _a, 'anomaly') 
    
    ax.set_xlabel(xlabel)
    plt.tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False) 
    plt.xticks(rotation=25)
    plt.legend();

    label(ax,5,80)
    plt.title(titlestring)
    plt.show(fig)


# ### Problems / Erwartungsmonitoring
# 
# - am 12.07.2022 wurde ein BIS Release installiert dass zur Verzögerung vieler Nachrichten geführt hat.
# Die letzten Nachrichten wurden am Folgetag um ca. 15:00 Uhr CET verarbeitet.
# Es sollten also deutliche Anomalien zwischen 12.07.2022 (10:00 Uhr) bis 13.07.2022 (15:00 Uhr) zu sehen sein.
# 
# 
# - 13.09.2022 19:00 Uhr / 20:00 Uhr UTC: Massive Last, Anflutung des Systems, vor allem im zweiten Intervall.
# Durchlaufzeiten waren deutlich erhöht. 
# 

# ### plot graphs

# In[33]:


def plot_graph(_df,contains=None, y='outcome'):
    _df2 = _df[_df.index.str.contains(contains)]

    fig = plt.figure(figsize=(18,9))
    
    hours= False
    
    plt.plot(_df2.index, _df2[y], label=y)
    if len(_df2.index[0].split('-')) == 4:
        hours = True
        ticks=_df2.index[_df2.index.str.contains('-00')]
        ticks2 = list(ticks)
        ticks2.append('2022-07-12-10')
        ticks2.append('2022-07-13-15')
        ticks2.sort()
        ticks = ticks2
    elif len(_df2.index[0].split('-')) == 3:
        ticks=list(_df2.index[_df2.index.str.contains('-01')])  
        for day in range(2,10):
            ticks.remove('2022-01-0' + str(day))
        for day in range(10,32):
            ticks.remove('2022-01-' + str(day)) 
        b=(len(ticks)-1)    
        ticks = ticks[:6] + ['2022-07-12'] + ticks[6:b]     
    else:
        ticks=_df2.index
    #ticks=[_df2.index[0]]
    
    #12.07.2022 (10:00 Uhr) bis 13.07.2022 (15:00 Uhr)
    if hours == False:
        plt.axvline(x='2022-07-12', color="red")
        plt.axvline(x='2022-09-13', color="red")
    else:    
         plt.axvline(x='2022-07-12-10', color="red")
         plt.axvline(x='2022-07-13-15', color="red")
         #plt.axvline(x='2022-09-13-19', color="red")
         #plt.axvline(x='2022-09-13-20', color="red")
       
    #print(ticks)
    
    plt.legend();
    plt.xticks(rotation=80,ticks=ticks)
    plt.show(fig)
    return ticks    



def plot_graph2(test=None, expect=None, column_y='outcome',contains=None, date_from=None, date_to=None):
        
    _test =test
    
    if contains is not None:
        _test = _test[_test.index.str.contains(contains)]
        #anomalies = anomalies[anomalies.index.str.contains(contains)]
    elif date_from is not None and date_to is not None:
        _from_index = _test.index.get_loc(date_from)
        _to_index   = _test.index.get_loc(date_to)
        #_df2 = _df2[_df2.index[_from_index:_to_index + 1]]
        _test = _test.iloc[_from_index:_to_index + 1]
    else:
        pass    
            
    fig = plt.figure(figsize=(18,9))
    plt.plot(_test.index, _test[column_y])       
    
    ticks = (_test.index[0], _test.index[len(_test.index) - 1])
    plt.xticks(rotation=80,ticks=ticks)
    
    if expect is not None:
        #print(_test.index.get_loc(expect[0]),_test.index.get_loc(expect[1]))
        e1=_test.index.get_loc(expect[0])
        e2=_test.index.get_loc(expect[1])
        x = np.arange(e1,e2)
        #x = np.arange(expect[0],expect[1])
        _max = np.max(_test[column_y])
        y1 = [0]*len(x)
        y2 = [_max]*len(x)
        plt.fill_between(x, y1, y2, facecolor='g', alpha=.3)
    plt.show()


# ### filtered plot

# In[34]:


def inverse_transform_anomalies(scaler=None,anomalies=None, column_y='close'):
    _a = anomalies[column_y]
    _a = pd.DataFrame(_a)
    _a = scaler.inverse_transform(_a)
    _a = _a[:, 0]
    return _a

def filtered_plot(test=None,scaler=None,anomalies=None,TIME_STEPS=None,contains=None, date_from=None, date_to=None,titlestring='',xlabel='',skip=None,rotate=80):
    #_df2 = test[TIME_STEPS:]
    _df2 = test
    
    if contains is not None:
        _df2 = _df2[_df2.index.str.contains(contains)]
        anomalies = anomalies[anomalies.index.str.contains(contains)]
    elif date_from is not None and date_to is not None:
        _from_index = _df2.index.get_loc(date_from)
        _to_index   = _df2.index.get_loc(date_to)
        #_df2 = _df2[_df2.index[_from_index:_to_index + 1]]
        _df2 = _df2.iloc[_from_index:_to_index + 1]
        
        anomalies = anomalies.iloc[_from_index:_to_index + 1]
        
        for date in anomalyEnc6.anomalies.index:
            if date >= '2022-07-01-00':
                _min = anomalyEnc6.anomalies.index.get_loc(date)
                break     
        for _max in range(_min,len(anomalyEnc6.anomalies.index)):
            if anomalyEnc6.anomalies.index[_max] > '2022-07-07-23':
                break
        anomalies = anomalies.iloc[_min:_max]         
    else:
        pass
    
    _a = inverse_transform_anomalies(scaler=scaler,anomalies=anomalies, column_y='close')
    
    fig = plt.figure(figsize=(30,12))

    plt.plot(
      _df2.index,  
      scaler.inverse_transform(_df2),   
      label='msg count'
    );

    
    
    ax = scatterplot(anomalies.index, _a, 'anomaly') 

    if skip is not None:
        label(ax,skip,rotate)
    
    hours= False
    if len(_df2.index[0].split('-')) == 4:
        hours = True
    if hours == False:
        plt.axvline(x='2022-07-12', color="red")
        #plt.axvline(x='2022-07-13', color="red")
        #plt.axvline(x='2022-09-13', color="red")
    else:    
        plt.axvline(x='2022-07-12-10', color="red")
        plt.axvline(x='2022-07-13-15', color="red")
        #plt.axvline(x='2022-09-13-19', color="red")
        #plt.axvline(x='2022-09-13-20', color="red")
        
    plt.title(titlestring)
    #plt.show(fig)     
        
def filtered_plot_enc(anomalyEnc, contains=None,date_from=None, date_to=None,titlestring='',xlabel='',skip=None,rotate=80):     
    return filtered_plot(anomalyEnc.test,anomalyEnc.scaler,anomalyEnc.anomalies,anomalyEnc.TIME_STEPS,contains,date_from, date_to,titlestring,xlabel,skip,rotate)   


# In[35]:


def filtered_plot2(test=None,scaler=None,anomalies=None,TIME_STEPS=None,contains=None, date_from=None, date_to=None,titlestring='',xlabel='',skip=None,rotate=80, annotations=True, ylabel='msg count'):
    #_df2 = test[TIME_STEPS:]
    _df2 = test
    
    if contains is not None:
        _df2 = _df2[_df2.index.str.contains(contains)]
        anomalies = anomalies[anomalies.index.str.contains(contains)]
    elif date_from is not None and date_to is not None:
        _from_index = _df2.index.get_loc(date_from)
        _to_index   = _df2.index.get_loc(date_to)
        #_df2 = _df2[_df2.index[_from_index:_to_index + 1]]
        _df2 = _df2.iloc[_from_index:_to_index + 1]
    else:
        pass
    
    _a = anomalies['close']
    _a = pd.DataFrame(_a)
    _a = scaler.inverse_transform(_a)
    _a = _a[:, 0]
    
    fig = plt.figure(figsize=(30,12))

    skip_x = label_skip(len(_df2.index))
    
    plt.plot(
      _df2.index,  
      scaler.inverse_transform(_df2),   
      label=ylabel
    );

    plt.xticks( _df2.index[0::skip_x],rotation=70,fontsize=10)
    plt.yticks(fontsize=10)
    
    skip_x = label_skip(len(anomalies.index))
    
    ax = scatterplot(anomalies.index, _a, 'anomaly') 

    #if skip is not None:
    #    label(ax,skip,rotate)
    
    hours= False
    if len(_df2.index[0].split('-')) == 4:
        hours = True
        
    if annotations == True:    
        if hours == False:
            plt.axvline(x='2022-07-12', color="red")
            #plt.axvline(x='2022-07-13', color="red")
            #plt.axvline(x='2022-09-13', color="red")
        else:    
            plt.axvline(x='2022-07-12-10', color="red")
            plt.axvline(x='2022-07-13-15', color="red")
            #plt.axvline(x='2022-09-13-19', color="red")
            #plt.axvline(x='2022-09-13-20', color="red")
        
    plt.title(titlestring)
    #plt.show(fig)     
        

def filtered_plot_enc2(anomalyEnc, contains=None,date_from=None, date_to=None,titlestring='',xlabel='',skip=None,rotate=80, annotations=True, ylabel='msg count'):     
    return filtered_plot2(anomalyEnc.test,anomalyEnc.scaler,anomalyEnc.anomalies,anomalyEnc.TIME_STEPS,contains,date_from, date_to,titlestring,xlabel,skip,rotate, annotations=annotations, ylabel=ylabel)   


# ### plot_error

# In[36]:


def plt_add_anomaly(_df2):
    hours= False
    if len(_df2.index[0].split('-')) == 4:
        hours = True
    if hours == False:
        plt.axvline(x='2022-07-12', color="red")
        #plt.axvline(x='2022-07-13', color="red")
        #plt.axvline(x='2022-09-13', color="red")
    else:    
        plt.axvline(x='2022-07-12-10', color="red")
        plt.axvline(x='2022-07-13-15', color="red")
        #plt.axvline(x='2022-09-13-19', color="red")
        #plt.axvline(x='2022-09-13-20', color="red")

import datetime as dt

from datetime import time
import datetime as dt

def add_datetime_column(dataframe):
    _date = []
    for i,row in dataframe.iterrows():
        _date.append(dt.datetime.strptime(i, "%Y-%m-%d-%H"))
    dataframe['date'] = _date    

def get_closest_datestring(datestring='2022-07-13-15',dates=None):
    date = datetime.datetime.strptime(datestring, "%Y-%m-%d-%H")
    #dates = anomalyEnc2.anomalies['date']
    _dt = min(dates, key=lambda d: abs(d - date))
    _date = make_2digits([_dt.date().month, _dt.date().day, _dt.time().hour])
    return  str(_dt.date().year) + '-' + str(_date[0])  + '-' + str(_date[1]) + '-' + str(_date[2])
        
def filter_dataframe(dataframe,contains=None, date_from=None, date_to=None):        
    if contains is not None:
        dataframe1 = dataframe[dataframe.index.str.contains(contains)]
    elif date_from is not None and date_to is not None:
        _from_index = dataframe.index.get_loc(date_from)
        _to_index   = dataframe.index.get_loc(date_to)
        dataframe1 = dataframe.iloc[_from_index:_to_index + 1]   
    return dataframe1
    
def plot_error(anomalyEnc,contains=None, date_from=None, date_to=None):
    fig = plt.figure(figsize=(18,9))
    #_test = anomalyEnc.test_score_df.iloc[2050:]
    _test = anomalyEnc.test_score_df
    
    _test = filter_dataframe(_test,contains=contains, date_from=date_from, date_to=date_to)
    
    _threshold= pd.unique(anomalyEnc.anomalies['threshold'])[0]
    
    plt.plot(_test.index, _test['loss'])   
    plt.axhline(y=_threshold, color="red") 
    
    #ticks = (_test.index[0], _test.index[len(_test.index) - 1])
    #plt.xticks(rotation=80,ticks=ticks)
    skip_x = label_skip(len(_test.index))
    plt.xticks( _test.index[0::skip_x],rotation=70,fontsize=10)
    plt.yticks(fontsize=10)
    
    plt_add_anomaly(_test)
    
    plt.show(fig)


# In[37]:


def update_threshold_for_error_in_timerange(enc,date_from='2022-07-12-10',date_to='2022-07-13-15'):
    _test = filter_dataframe(enc.test_score_df,date_from='2022-07-12-10',date_to='2022-07-13-15')
    _threshold = _test.iloc[0]['loss']
    _anomalies = get_anomalies(enc,_threshold).index


# In[38]:


def update_thresholds():
    update_threshold_for_error_in_timerange(anomalyEnc_hour,date_from='2022-07-12-10',date_to='2022-07-13-15')
    update_threshold_for_error_in_timerange(anomalyEnc_CMESSAGETAT2_hour,date_from='2022-07-12-10',date_to='2022-07-13-15')


# #### SAVE anomalies, errors

# In[39]:


def save_errors():
    _path = '/home/jovyan/work/output/experiment_anomaly_expect/' + str(sender) + '/anomalyEnc_hour/'
    _enc = anomalyEnc_hour
    _enc.anomalies.to_parquet(_path + 'anomalies.parquet')
    _enc.test_score_df.to_parquet(_path + 'test_score_df.parquet')

    _path = '/home/jovyan/work/output/experiment_anomaly_expect/' + str(sender) + '/anomalyEnc_CMESSAGETAT2_hour/'
    _enc = anomalyEnc_CMESSAGETAT2_hour
    _enc.anomalies.to_parquet(_path + 'anomalies.parquet')
    _enc.test_score_df.to_parquet(_path + 'test_score_df.parquet')


# ### plot_anomalies

# In[40]:


def plot_bounding_lines(timerange,color="red"):
    hours= False
    if len(timerange[0].split('-')) == 4:
        hours = True
    if hours == False:
        plt.axvline(x=timerange[0], color=color)
    else:    
        plt.axvline(x=timerange[0], color=color)
        plt.axvline(x=timerange[1], color=color)

def plot_bounding_box(timerange,_test,scaler):        
    e1=_test.index.get_loc(timerange[0])
    e2=_test.index.get_loc(timerange[1])
    x = np.arange(e1,e2)
    _max = np.max(scaler.inverse_transform( _test))
    y1 = [0]*len(x)
    y2 = [_max]*len(x)
    plt.fill_between(x, y1, y2, facecolor='g', alpha=.3)
   

def plot_anomalies(anomalyEnc,test=None, scaler=None, expect=None, column_y='outcome',contains=None, date_from=None, date_to=None):
    _anomalies = anomalyEnc.anomalies
    _threshold= pd.unique(anomalyEnc.anomalies['threshold'])[0]
    
    _from = get_closest_datestring(datestring=date_from, dates = anomalyEnc.anomalies['datetime'])
    _to = get_closest_datestring(datestring=date_to,dates = anomalyEnc.anomalies['datetime'])
    _anomalies = filter_dataframe(anomalyEnc.anomalies, date_from=_from, date_to=_to)
    
    if test is None:
        _test = anomalyEnc.test
    else:
        _test = test
        
    if scaler is None:
        _scaler = anomalyEnc.scaler
    else:
        _scaler = scaler    
        
    _test = filter_dataframe(_test,contains=contains, date_from=date_from, date_to=date_to)     
    del _test["datetime"]    
        
    fig = plt.figure(figsize=(18,9))
    
    if expect is not None:
        plot_bounding_box(expect, _test, anomalyEnc.scaler)
        #plot_bounding_lines(expect)
    
    
    #plt.plot(_test.index, _test[column_y])       
    plt.plot(
      _test.index,  
      _scaler.inverse_transform( _test),   
      label='msg count'
    );

    _a = inverse_transform_anomalies(scaler=_scaler,anomalies=_anomalies, column_y='close')
    ax = scatterplot(_anomalies.index, _a, 'anomaly') 
    
    #ax = scatterplot(_anomalies.index, _anomalies[column_y], 'anomaly') 
    
    #ticks = (_test.index[0], _test.index[len(_test.index) - 1])
    #plt.xticks(rotation=80,ticks=ticks)
    
    skip_x = label_skip(len(_test.index))
    plt.xticks( _test.index[0::skip_x],rotation=70,fontsize=10)
    plt.yticks(fontsize=10)
    
   
    plt.show()


# In[41]:


#_test = plot_anomalies(enc,date_from='2022-07-08-10',date_to='2022-07-14-15',expect=('2022-07-12-10','2022-07-13-15'))


# In[42]:


#!ls -l /home/jovyan/work/output/experiment_anomaly_expect/


# # Loop

# In[43]:


def process(sender):
    pfall = df[df['CSENDERENDPOINTID'] == sender]
    #print('1')
    create_data(pfall)
    #print('2')
    train_models()
    save_models()
    update_thresholds()
    save_errors()


# In[50]:


#pfall


# In[44]:


#global sender
#sender = senders[2]
#process(sender)


# In[45]:


_path = '/home/jovyan/work/output/experiment_anomaly_expect/' + str(sender) + '/anomalyEnc_hour/'


# In[46]:


from os import listdir

def listdirectory(path=None,filter='.'):
    return [x for x in listdir(path) if not x.startswith(filter)]    

_path = '/home/jovyan/work/output/experiment_anomaly_expect/'
_files = listdirectory(path=_path)


# In[ ]:


for sender in senders:
    if str(sender) not in _files:
        try:
            process(sender)
        except Exception as exception:
            pass    

