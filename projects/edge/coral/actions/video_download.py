#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import video_actions_v2 as va


# In[ ]:


from os import listdir
_filename = '../data/videos/single/' + listdir('../data/videos/single')[1]
_sources = va.LiveVideoSources(_filename)
_id = list(_sources.videos.keys())[0]
_action = va.VideoAction(sources=_sources, id=_id, index=5)
_action.evaluate()['image'].save('/home/jovyan/work/images/test.png')

