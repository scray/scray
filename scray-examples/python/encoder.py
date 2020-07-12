#!/usr/bin/env python
# coding: utf-8

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

