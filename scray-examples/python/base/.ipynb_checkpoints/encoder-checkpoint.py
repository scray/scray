#!/usr/bin/env python
# coding: utf-8

# In[4]:


#https://stackoverflow.com/questions/50041551/tell-labelenocder-to-ignore-new-labels

import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.utils.validation import check_is_fitted
from sklearn.utils.validation import column_or_1d

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

    # for appended versions
    def transform_version(self, y):
        results = self.transform(y)
        index = np.where(results == -1)[0]
        with_prefix = ['zzzz_0001_' + y[i] for i in index]
        results_with_prefix = self.transform(with_prefix)
        for i,r in enumerate(results_with_prefix):
            results[index[i]] = r
        return results    
    
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

    # problem with None
    def dict(self):
        return dict(zip(self.classes_, self.transform(self.classes_)))
    
    def diff(self,values):
        _encoded = list(_encoder.classes_)
        try:
            _encoded.remove(None)
        except Exception as exception:
            pass
        return list(set(values) - set(_encoded))
    
    def append(self,values, version='0001'):
        self.classes_ = np.append(self.classes_, ['zzzz' + '_' + version + '_' + s  for s in list(np.sort(list(values)))])


# In[5]:


def createEncoders(dataall,columns):
    for column in columns:
        le = TolerantLabelEncoder(ignore_unknown=True)
        #le.fit([1, 2, 2, 6])
        le.fit(dataall[column])
        LabelEncoder()
        print(le.classes_)
        np.save(column + '.npy', le.classes_)
        
def encode(dataall,columns,npy='/home/jovyan/work/npy/'):
    # save np.load
    np_load_old = np.load

    # modify the default parameters of np.load
    np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)

    for column in columns:
        encoder = TolerantLabelEncoder(ignore_unknown=True)
        encoder.classes_ = np.load(npy + column + '.npy')
        dataall[column] = encoder.transform(dataall[column]) 

    # restore np.load for future normal usage
    np.load = np_load_old
    
def getEncoder(column,npy='/home/jovyan/work/npy/'):
    # save np.load
    np_load_old = np.load

    # modify the default parameters of np.load
    np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)

    encoder = TolerantLabelEncoder(ignore_unknown=True)
    encoder.classes_ = np.load(npy + column + '.npy')
        
    # restore np.load for future normal usage
    np.load = np_load_old
    return encoder

