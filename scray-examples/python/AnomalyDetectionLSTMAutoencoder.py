#!/usr/bin/env python
# coding: utf-8

# In[3]:


## Setup charts
import pandas as pd
pd.plotting.register_matplotlib_converters()
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')
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

def get_ymd_string(a,b,c) :
    return a + "-" + b + "-" + c 

def get_ymdh(mdcountsall) :
    a = mdcountsall.index.get_level_values(0).astype(str)
    b = mdcountsall.index.get_level_values(1).astype(str)
    c = mdcountsall.index.get_level_values(2).astype(str)
    d = mdcountsall.index.get_level_values(3).astype(str)
    return a,b,c,d

def get_ymdh_string(a,b,c,d) :
    return a + "-" + b + "-" + c + "-" + d

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

def createData(pfall,month) :
    if (month > 0) & (month < 13) :
        mdcountsall = pfall[(pfall['month'] == month)].groupby(['year','month','day','hour'])['year'].count()
    else :
        mdcountsall = pfall.groupby(['year','month','day','hour'])['year'].count()    
    a,b,c,d = get_ymdh(mdcountsall)
    data2 = pd.DataFrame()
    data2['date'] = get_ymdh_string(a,b,c,d)
    data2['outcome'] =  mdcountsall.reset_index(level=0, drop=True).reset_index()['year'].astype(int)

    #for pivot table
    data2['hours'] =  d.astype(int) 
    data2['days']  =  c.astype(int) 
    return data2

def createData_ymd(pfall,month) :
    if (month > 0) & (month < 13) :
        mdcountsall = pfall[(pfall['month'] == month)].groupby(['year','month','day'])['year'].count()
    else :
        mdcountsall = pfall.groupby(['year','month','day'])['year'].count()    
    a,b,c = get_ymd(mdcountsall)
    data2 = pd.DataFrame()
    data2['date'] = get_ymd_string(a,b,c)
    data2['outcome'] = mdcountsall.reset_index(level=0, drop=True).reset_index()['year'].astype(int) 
    return data2

def label(graph,skip,rot) :
    for ind, label in enumerate(graph.get_xticklabels()):
        if ind % skip == 0:  # every 10th label is kept
            label.set_visible(True)
            label.set_rotation(rot)
        else:
            label.set_visible(False)

def createBarplot(md,fx,fy,fontscale,title="") :
    sns.set(style='whitegrid', palette='muted', font_scale=fontscale)
    plt.figure(figsize=(fx,fy))
    plt.title(title)
    ax = sns.barplot(x=md['date'], y=md['outcome'], data=md)
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


# In[4]:


import numpy as np
import tensorflow as tf
from tensorflow import keras
import pandas as pd
import seaborn as sns
from pylab import rcParams
import matplotlib.pyplot as plt
from matplotlib import rc
from pandas.plotting import register_matplotlib_converters
from sklearn.preprocessing import StandardScaler

class AnomalyDetectionLSTMAutoencoder():
    def __init__(self,OUTCOME = 'close', TIME_STEPS = 24):
        #%matplotlib inline
        #%config InlineBackend.figure_format='retina'

        self.OUTCOME    = OUTCOME
        self.TIME_STEPS = TIME_STEPS
        
        register_matplotlib_converters()
        sns.set(style='whitegrid', palette='muted', font_scale=1.5)

        rcParams['figure.figsize'] = 22, 10

        RANDOM_SEED = 42

        np.random.seed(RANDOM_SEED)
        tf.random.set_seed(RANDOM_SEED)
        
    def init_sns(self):
        sns.set(style='whitegrid', palette='muted', font_scale=1.5)
        rcParams['figure.figsize'] = 22, 10  
        
    # setup data (current)
    def createDataframe(self,pfall) :
        data3 = createData(pfall,0)
        df = pd.DataFrame()
        OUTCOME = self.OUTCOME
        df[OUTCOME] = data3['outcome']
        df.set_index(data3['date'], inplace=True)
        return df

    def getTrainAndTest(self,df,TRAIN_SIZE) :
        train_size = int(len(df) * TRAIN_SIZE)
        test_size = len(df) - train_size
        train, test = df.iloc[0:train_size], df.iloc[train_size:len(df)]
        print("train.shape: ",train.shape, "test.shape: ", test.shape)
        return train, test

    def create_dataset(self,X, y, time_steps=1):
        Xs, ys = [], []
        for i in range(len(X) - time_steps):
            v = X.iloc[i:(i + time_steps)].values
            Xs.append(v)        
            ys.append(y.iloc[i + time_steps])
        return np.array(Xs), np.array(ys)

    def initmodel(self, X_train):
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

    def testScoreDF(self,model, THRESHOLD,X_test,test) : 
        X_test_pred = model.predict(X_test)
        test_mae_loss = np.mean(np.abs(X_test_pred - X_test), axis=1)

        test_score_df = pd.DataFrame(index=test[self.TIME_STEPS:].index)
        test_score_df['loss'] = test_mae_loss
        test_score_df['threshold'] = THRESHOLD
        test_score_df['anomaly'] = test_score_df.loss > test_score_df.threshold
        test_score_df[self.OUTCOME] = test[self.TIME_STEPS:][self.OUTCOME]
        return test_score_df
    
    
    def initAndTrain(self,pfall1,perc_train):
        self.df1 = self.createDataframe(pfall1)
        self.train, self.test = self.getTrainAndTest(self.df1,perc_train)

        self.scaler = StandardScaler()
        self.scaler = self.scaler.fit(self.train[[self.OUTCOME]])
        self.train[self.OUTCOME] = self.scaler.transform(self.train[[self.OUTCOME]])
        self.test[self.OUTCOME] = self.scaler.transform(self.test[[self.OUTCOME]])

        # reshape to [samples, time_steps, n_features]

        self.X_train, self.y_train = self.create_dataset(self.train[[self.OUTCOME]], self.train.close, self.TIME_STEPS)
        self.X_test, self.y_test = self.create_dataset(self.test[[self.OUTCOME]], self.test.close, self.TIME_STEPS)
        #print(X_train.shape)

        self.model = self.initmodel(self.X_train)

        history = self.model.fit(
            self.X_train, self.y_train,
            epochs=10,
            batch_size=32,
            validation_split=0.1,
            shuffle=False)

        self.X_train_pred = self.model.predict(self.X_train)
        self.train_mae_loss = np.mean(np.abs(self.X_train_pred - self.X_train), axis=1)


# In[2]:





# In[4]:





# In[ ]:





# In[ ]:





# In[ ]:


TIME_STEPS = 24

def plot_test(test,scaler,anomalies,titlestring):

    plt.plot(
      test[TIME_STEPS:].index, 
      scaler.inverse_transform(test[TIME_STEPS:].close), 
      label='msg count'
    );

    ax = sns.scatterplot(
      anomalies.index,
      scaler.inverse_transform(anomalies.close),
      color=sns.color_palette()[3],
      s=152,
      label='anomaly'
    )
    plt.xticks(rotation=25)
    plt.legend();

    label(ax,5,80)


    plt.title(titlestring)

