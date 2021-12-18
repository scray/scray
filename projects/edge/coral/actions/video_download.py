#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!pip3 install opencv-python
#!pip install git+https://github.com/Cupcakus/pafy
#!pip install youtube_dl


# In[1]:


import video_actions_v2 as va

import time, datetime
from os import listdir

def listdirectory(directory,filter='.'):
    return [x for x in listdir(directory) if not x.startswith(filter)]

_videos = listdirectory('../data/videos/single')

while True:
    for _video in _videos:
        _filename = '../data/videos/single/' + _video
        #print(_filename)
        _sources = va.LiveVideoSources(_filename)
        _id = list(_sources.videos.keys())[0]
        _action = va.VideoAction(sources=_sources, id=_id, index=5)
        ts = time.time()
        asctime = time.asctime().split(' ', 1)[1].replace(' ','-')
        _action.evaluate()['image'].save('/home/jovyan/work/images/' + _id + '_' + str(_action.index) + '_' + str(ts) + '_' + asctime + '_' + '.png')


# In[ ]:


_action.__dict__


# In[ ]:


_action = va.VideoAction(sources=_sources, id=_id, index=5)

