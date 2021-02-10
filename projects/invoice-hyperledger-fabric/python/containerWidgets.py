#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# widgets Tool

import ipywidgets as widgets
from IPython.display import clear_output
from IPython.display import Javascript, display
import uuid

style = {'description_width': '250px'}
layout = {'width': '500px'}
 
def addVisText(key='',value='',disabled=False, layout=layout):
    text = widgets.Text(description = key,value = value, style=style, layout=layout,disabled=disabled)
    display(text)  
    return text
    
def addVisCheckbox(key='',value=False):
    text = widgets.Checkbox(description = key,value = value, style=style, layout=layout)
    display(text)   
    return text

def addHeader(text='',bold=True):
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

