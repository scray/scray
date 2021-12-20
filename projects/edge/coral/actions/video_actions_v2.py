#!/usr/bin/env python
# coding: utf-8

# # ImageWidget

# In[2]:


import cv2
import numpy as np
from PIL import Image
import io

style = {'description_width': '250px'}
layout = {'width': '500px'}

class ImageWidget(object):
    def __init__(self, image=None, display=True):
        self.count = 0
        
        self.image_w = widgets.Image(format='PNG')
        self.update_w = widgets.Checkbox(description='update',value=True)
        self.counter_w = widgets.Text(description = 'count',value = '', style=style, layout=layout,disabled=False)
        VBox      = widgets.VBox([self.update_w,self.counter_w])  
        self.HBox = widgets.HBox([self.image_w, VBox])  
        
        self.setImage(image)
        if display == True:
            self.display()
    
    def display(self):
        #display(self.image_w)  
        display(self.HBox) 
    
    def getByteArray(self,image):    
        #return None
        img_byte_arr = io.BytesIO()
        image.save(img_byte_arr, format='PNG')
        img_byte_arr = img_byte_arr.getvalue()
        return img_byte_arr
    
    def evaluate(self, image=None):
        self.setImage(image)
        
    def _dict(self):
        return {'class' : self.__class__.__name__, 'parameters' : {}}
    
    def setImage(self,image):
        #print('ImageWidget')
        self.count += 1
        self.counter_w.value = str(self.count) 
        
        if self.update_w.value == False:
            return
        
        if image != None:
            try:
                #print('set image', type(image))
                if isinstance(image, dict):
                    image = image['image']
                self.image_w.value = self.getByteArray(image)    
            except Exception as exception: 
                print('Exception ImageWidget', str(exception))
            
    def getImage(self):
        return Image.open(io.BytesIO(self.image_w.value))


# # Actions and Widgets 

# ## BaseAction

# In[3]:


class BaseAction(object):
    
    def __init__(self):
        self.name = __class__.__name__
    
    def dict(self):
        return {'class' : self.__class__.__name__, 'parameters' : self.__dict__}
    
    def merged_dict(self,parameters=None):
        old = dict()
        return {'class':old['class'],'parameters':{**old['parameters'],**parameters}}
    
    def merge(self,parameters={}):
        adict = {'class' : self.__class__.__name__, 'parameters' : {**self.__dict__, **parameters}}
        #adict = merged_dict(parameters)
        return eval(adict['class'])(**adict['parameters'])
    
    def _get_image(self,image):
        if isinstance(image, dict):
            return image['image']
        return image
    
    def _filename(self,filename=None,name=None):
        if name != None:
            return name + '_' + self.__class__.__name__ + '.json'
        if filename == None:
            return '_action_' + self.__class__.__name__ + '.json'
        return filename
    
    def save(self,filename=None,name=None):
        with open(self._filename(filename,name), 'w', encoding='utf-8') as f:
            json.dump(self.dict(), f, ensure_ascii=False, indent=4)    

    def load(self,filename=None,name=None):
        with open(self._filename(filename,name), 'r') as fp:
            file = json.load(fp)  
            return file          
        
   


# ## BaseWidget

# In[4]:


class BaseWidget(object):
    def evaluate(self,image=None):
        return self.action.evaluate(image)
    
    def _get_image(self,image):
        if isinstance(image, dict):
            return image['image']
        return image  
    
     # write output    
    def output_result(self,update=True, result=None):
        if update == True:
            if(self.imageWidget != None):
                self.imageWidget.evaluate(result)    
                
    def get_result(self):
        if hasattr(self, 'result'):
            return self.result
        return None 


# ## TemplateAction

# In[5]:


class TemplateAction(BaseAction):
    def __init__(self):
        pass
    
    def evaluate(self, **kwargs):        
        if 'image' in kwargs.keys():
            self.result = {'image':kwargs['image']}
        return self.result


# ## TemplateWidget

# In[6]:


class TemplateWidget(BaseWidget):
    def __init__(self, action=None, imageWidget=None, parent=None):
        self.action = action
        self.parent = parent
        self.result = None
        self.imageWidget = imageWidget
        self.vbox   = widgets.VBox([])
        
        if action == None:
            action = TemplateAction()
            
    def evaluate(self, **kwargs):        
        if 'image' in kwargs.keys():
            self.result = {'image':kwargs['image']}
        return self.result
            
    def myfunc(self,**kwargs):
        # kwargs is a dictionary.
        for k,v in kwargs.items():
             print ("%s = %s" % (k, v))
        if 'image' in kwargs.keys():
            print('found')        


# ## CropAction

# In[7]:


# input: image, output cropped image
class CropAction(BaseAction):
    WORLD = 0
    NORMALIZED = 1
    # __init__ is a special method called whenever you try to make
    # an instance of a class. As you heard, it initializes the object.
    # Here, we'll initialize some of the data.
    #def __init__(self, image=None, size=None, min=[0.0,0.0], max=[1.0,1.0]):
    #def __init__(self,size=None, min=None, max=None,region=None,mode=WORLD):
    def __init__(self,size=None, min=[0,0], max=[1,1],region=None,mode=WORLD):    
        self.min    = min
        self.max    = max
        self.region = None
        self.size   = size
        self.mode   = mode
        
    def dict(self):
        # return self.toNormalized().super(CropAction,self).__init__().dict()
        #return self.toNormalized().dict()
        return super(CropAction, self.toNormalized()).dict()
        
    # Problem: + 1    
    def evaluate(self, image=None):
        image = self._get_image(image)
        
        if self.mode == self.WORLD:
            return {'image':image.crop(self._flat())}
        return {'image':image.crop(self.fromNormalized(self.flat(), size=image.size))}      
        
    def _flat(self):
        return (self.min[0],self.min[1],self.max[0] + 1,self.max[1] + 1)
    
    def flat(self):
        return (self.min[0],self.min[1],self.max[0],self.max[1])
    
    def normalizeValue(self, value, size):
        return float (value) / size

    def fromNormalizedValue(self, value,size):
        return int(value * size)

    def normalized(self):    
        #print(self.min, self.max, self.size)
        xmin = self.normalizeValue(self.min[0],self.size[0])
        ymin = self.normalizeValue(self.min[1],self.size[1])
        xmax = self.normalizeValue(self.max[0],self.size[0])
        ymax = self.normalizeValue(self.max[1],self.size[1])
        return (xmin,ymin,xmax,ymax)    

    def toNormalized(self):
        if self.mode == self.NORMALIZED:
            return self
        _norm = self.normalized()
        return CropAction(min=[_norm[0],_norm[1]], max=[_norm[2],_norm[3]], mode=self.NORMALIZED)
    
    def toWorld(self,size):
        if self.mode == self.WORLD:
            return self
        
       
        _world = self.fromNormalized(self.flat(),size=size)
        #print('CropAction',_world)
        return CropAction(min=[_world[0],_world[1]], max=[_world[2],_world[3]], size=size, mode=self.WORLD)
    
    def fromNormalized(self, values, size=None):    
        if size == None:
            size = self.size
             
        # init size to fix initMinMax problem --> is this really required
        self.size = size        
                
         
        xmin = self.fromNormalizedValue(values[0],size[0])
        ymin = self.fromNormalizedValue(values[1],size[1])
        xmax = self.fromNormalizedValue(values[2],size[0])
        ymax = self.fromNormalizedValue(values[3],size[1])
       
        return [xmin,ymin,xmax,ymax]
    
    def update(self, size=None):
        to = self.normalized()
        self.size = size
        self.min[0] = self.fromNormalizedValue(to[0],size[0])
        self.min[1] = self.fromNormalizedValue(to[1],size[1])
        self.max[0] = self.fromNormalizedValue(to[2],size[0]) 
        self.max[1] = self.fromNormalizedValue(to[3],size[1]) 


# ## DisplayWidget
# action: VideoAction (uses only VideoAction)

# In[8]:


class DisplayWidget(BaseWidget):
    def __init__(self, action=None, update=None):
        self.action       = action
        self.index        = 0  # for change --> last index
        self.image        = None
        self.update       = update
        
        if action != None:
            self.init(action)
        
        _max = 5
        layout2={'width': '300px'}
        self.imageselectw = widgets.IntSlider(value=self.index, min=0,max=_max,step=1, description='image',layout=layout)
        self.imageselectw.observe(self.on_value_change_imageselectw, 'value')
        self.next_image_button = widgets.Button(description='Next', disabled=False, tooltip='next image',style=style)
        self.next_image_button.on_click(self.on_next_image_button_clicked)  
        self.skipframes = widgets.Text(description   = 'skip frames',value = '10', style=style, layout=layout2)
        self.skipframes.on_submit(self.on_value_submit_skipframes)
        image_select_hbox = widgets.HBox([self.imageselectw,self.next_image_button,self.skipframes])
        self.hbox=image_select_hbox
        
        self.init(action)
        
    def init(self,action=None):
        if action != None:
            self.action = action
            self.index  = action.index
    
    def on_value_submit_skipframes(self,change):
        if self.action != None:
            self.action.skipframes = int(change.value)
        
    # select image slider
    def on_value_change_imageselectw(self,change):
        self.index = change['new']
        self.action.setIndex(change['new'])
        
        self.image = self.action.evaluate()['image']
        if self.update != None:
            self.update(self.image,update=True)
        
    def on_next_image_button_clicked(self,b):
        if self.action.index != self.index:
            #self.action = VideoAction(video=self.action.video,index=self.index)
            self.action.setIndex(self.index)
        self.action.skipframes = int(self.skipframes.value)
        self.image = self.action.evaluate()['image']
        if self.update != None:
            self.update(self.image,update=True)


# ## CropWidget
# 
# abhängig von parent und sources (speichert regions dort)

# In[9]:


#display(DisplayWidget(action=_crop.videoAction,imageWidget=_imageWidget).hbox)
class CropWidget(BaseWidget):
    def __init__(self, action=None, videoAction=None ,parent=None, imageWidget=None, image=None):
        self.result       = None
        self.action       = action
        self.image        = image    # buffers image to be cropped
        self.parent       = parent
        
        self.imageWidget  = imageWidget # None or used to display result
        self.displayWidget = None       # TODO: remove
    
        self.update = True 
    
        # still needed ?
        cropNormalizedArea = [0.0,0.0,1.0,1.0]    
        self.regions = dict()
        self.regions['reset'] = cropNormalizedArea
    
        if image != None:
            action.size = image.size
    
        self.reset_button = widgets.Button(description='Reset', disabled=False, tooltip='reset all values',style=style)
        self.reset_button.on_click(self.on_reset_button_clicked)  

        self.crop_name = widgets.Text(description = 'name',value = '', style=style, layout=layout,disabled=False)
        self.crop_name.on_submit(self.on_value_submit_crop_name)
        self.crop_list = widgets.Dropdown(description='region',options=self.regions.keys())
        self.crop_list.observe(self.crop_list_on_change) 
        self.crop_bookmarks = widgets.HBox([self.crop_list, self.crop_name, self.reset_button])
        
        ##########################################################################    
    
        self.crop_size_x = widgets.Text(description = 'x',value = '', style=style, layout=layout,disabled=False)
        self.crop_size_y = widgets.Text(description = 'y',value = '', style=style, layout=layout,disabled=False)
        size_vbox   = widgets.VBox([self.crop_size_x, self.crop_size_y])    
        
        #self.cropx0 = self._IntSlider(description='x0',value=0)
        #self.cropx1 = self._IntSlider(description='x1')
        #self.cropx0 = widgets.IntSlider(value=0, step=1, description='x0',layout=layout)
        #self.cropx1 = widgets.IntSlider(value=0, step=1, description='x1',layout=layout)
        #self.cropx0.observe(self.on_value_change_crop_xmin, 'value')
        #self.cropx1.observe(self.on_value_change_crop_xmax, 'value')
        
        #self.crop_hbox_x = widgets.HBox([self.cropx0, self.cropx1])
        
        self.crop_hbox_x = widgets.HBox()
        self.crop_hbox_y = widgets.HBox()
        self._addIntCropSliders()
        
        vbox1 = widgets.VBox([self.crop_bookmarks,self.crop_hbox_x, self.crop_hbox_y])
        hbox  = widgets.HBox([vbox1, size_vbox])
        self.vbox   = widgets.VBox([hbox])
        
        # set size of image
        if action != None and videoAction != None:
            self._action_to_world(videoAction.stream.dimensions)
        
        if action != None and action.size != None:
            self.initMinMax(action.size,(0,0,action.size[0]-1,action.size[1]-1))
        
        
    ### SLIDER ############################################################    
    def _addIntCropSliders(self):
        self.cropx0 = self._IntSlider(description='x0')
        self.cropx1 = self._IntSlider(description='x1')
        self.cropx0.observe(self.on_value_change_crop_xmin, 'value')
        self.cropx1.observe(self.on_value_change_crop_xmax, 'value')
        self.crop_hbox_x.children = (self.cropx0,self.cropx1)
        
        self.cropy0 =  self._IntSlider(description='y0')
        self.cropy1 =  self._IntSlider(description='y1')
        self.cropy0.observe(self.on_value_change_crop_ymin, 'value')
        self.cropy1.observe(self.on_value_change_crop_ymax, 'value')
        self.crop_hbox_y.children = (self.cropy0,self.cropy1)
        
    def _FloatSlider(self,description='',value=0.0,layout=layout):
        return widgets.FloatSlider(value=value,description=description, min=0, max=1.0, step=0.001, readout_format='.3f',layout=layout)
    
    def _IntSlider(self,description='',value=0,max=0,layout=layout):
        return widgets.IntSlider(value=value, max=max,step=1, description=description,layout=layout)    
        
        
    # Hilfsfunktion, es gibt nur eine VideoACtion    
    def _get_VideoAction(self):
        return self.displayWidget.action
        
    def _action_to_world(self,size):
        if(self.action.mode != self.action.WORLD):
            self.action = self.action.toWorld(size=size)    
        
    
        
    ##########################################################################       
        
    # INIT    ---> creates new action !!! sets parameters for widget, depends on other actions
    # VideoAction oder parent ??????
    
    #def init(self, action=None, parent=None):
    def init(self, action=None):    
        #self.parent = parent
        self.crop_list.options=action.sources.videos[action.id]['bookmarks'].keys()
        
        ## TODO: remove ------> outside
        if self.displayWidget != None:
            self.displayWidget.init(action) 
            
        self.initMinMax()
   
    def evaluate(self,image=None):
        # Achtung schreibt image auch in ImageWidget !!!!!!! Doppelt
        #print('CropWidget', 'evaluate')
        self.updateImage(image)
        return self.result
    
    # after image is updated ######### size or image changed / source
    # called e.g. by DisplayWidget
    def updateImage(self,image=None,update=False):
        self.image = self._get_image(image)
        size = self.image.size
        
        if(self.action == None):
            self.action   = CropAction(size=size,min=[0,0], max=[size[0],size[1]])
        
       
        # action is normalized until size of image is known    
        self._action_to_world(size)
          
        if(self.action.size != size or self.cropx1.max != (size[0] - 1)):
            self.action.update(size)
            self.crop_image()
            self.initMinMax()
        else:
            self.crop_image(update=update)  
            
        self.crop_size_x.value = str(self.action.max[0] - self.action.min[0])
        self.crop_size_y.value = str(self.action.max[1] - self.action.min[1])
     
    # RESULT 
    def crop_image(self, update=False):
        image=None
        
        if self.image != None :  
            image = self.image
        elif self.parent != None:
            image = self.parent.get_result()
        
        if image != None:
            if(self.action == None):
                self.action   = CropAction(size=image.size,min=[0,0], max=[image.size[0],image.size[1]])
            
            self.result = self.action.evaluate(image=image)
            
            # write output
            if self.update == True:
                self.output_result(update=update,result=self.result)
                
            #if update == True:
            #    if(self.imageWidget != None):
            #        self.imageWidget.evaluate(self.result['image'])           
        
    def initMinMax(self,size=None,cropArea = None):
        #print('CropWidget','initMinMax')
        if self.action == None:
            return
        
        if size == None:
            size = self.action.size
            self._action_to_world(size)
            cropArea = self.action.flat()
        else:
            self._action_to_world(size)
        
        self.update = False
        
        self.cropx0.max = size[0] - 1
        self.cropx1.max = size[0] - 1
        self.cropy0.max = size[1] - 1
        self.cropy1.max = size[1] - 1

        self.cropx0.value = cropArea[0]
        self.cropy0.value = cropArea[1]
        self.cropx1.value = cropArea[2]
        
        # update image only once
        self.update = True
        self.cropy1.value = cropArea[3]    
        
    def on_value_change_crop_xmin(self,change):
        self.cropx1.min  = change['new']
        self.action.min[0] = change['new']
        self.crop_image(update=True)
            
    def on_value_change_crop_xmax(self,change):
        self.cropx0.max  = change['new']
        self.action.max[0] = change['new']
        self.crop_image(update=True)

    def on_value_change_crop_ymin(self,change):
        self.cropy1.min  = change['new']
        self.action.min[1] = change['new']
        self.crop_image(update=True)

    def on_value_change_crop_ymax(self,change):
        self.cropy0.max  = change['new']
        self.action.max[1] = change['new']
        self.crop_image(update=True)        
        
        
        
    # add/update bookmark of current video source
    def add_bookmark(self,name):
        self._get_VideoAction().sources.videos[self._get_VideoAction().id]['bookmarks'][name] = self.action.normalized()
        
        if name not in self.crop_list.options:
            self.crop_list.options = list(self.crop_list.options) + [name] 
        self.crop_list.value = name
    
    def remove_bookmark(self,name):
        del(self._get_VideoAction().sources.videos[self._get_VideoAction().id]['bookmarks'][name])
        
        _list = list(self.crop_list.options)
        if 'test3' in _list: _list.remove('test3')
        self.crop_list.options = _list
        self.crop_list.value = _list[len(_list)-1]
    
    def on_value_submit_crop_name(self,change):
        self.add_bookmark(change.value)
        
    def crop_list_on_change(self,change):
        if change['type'] == 'change' and change['name'] == 'value':
            self.crop_name.value = change['new']
            #self.initMinMax(self.action.size,self.action.fromNormalized(self.parent.sources.videos[self.displayWidget.action.id]['bookmarks'][change['new']]))
            self.initMinMax(self.action.size,self.action.fromNormalized(self._get_VideoAction().sources.videos[self._get_VideoAction().id]['bookmarks'][change['new']]))
            
    def on_reset_button_clicked(self,b):
        self.action.min = [0,0]
        self.action.max = [self.image.size[0],self.image.size[1]]
        self.initMinMax()
        self.crop_name.value = ''


# ## ResizeAction

# In[10]:


####################### scale
options={'NEAREST' : Image.NEAREST,'BOX' : Image.BOX,'BILINEAR' : Image.BILINEAR,'HAMMING' : Image.HAMMING,'BICUBIC' : Image.BICUBIC,'LANCZOS' : Image.LANCZOS}
#options={Image.NEAREST,Image.BOX,Image.BILINEAR,Image.HAMMING,Image.BICUBIC,Image.LANCZOS}
#size_options = {(128,128),(224,224),(240,240),(256,256),(299,299),(300,300),(320,320),(513,513)}
size_options = {'(128,128)' : (128,128), '(224,224)' : (224,224), '(240,240)' : (240,240), '(256,256)' : (256,256),
                '(299,299)' : (299,299),'(300,300)' : (300,300),'(320,320)' : (320,320),'(513,513)' : (513,513),
                '(640,480)' : (640,480)}


class ResizeAction(BaseAction):
    def __init__(self,size=None,use_w=False,use_h=False,algorithm=Image.NEAREST):
        self.size=size
        self.use_w=use_w
        self.use_h=use_h
        self.algorithm=algorithm
        
    def evaluate(self,image=None):
        try:
            w = self.size[0]
            h = self.size[1]
            #print(size,w,h)
            imagea = image

            ratio = h / imagea.size[1]
            newsize = (int(ratio * imagea.size[0]), int(ratio * imagea.size[1]) )

            ratio = w / imagea.size[0]
            h2 = int(ratio * imagea.size[1])
            w2 = int(ratio * imagea.size[0])
            if h2 > h:
                ratio = h / imagea.size[1]
                h2 = int(ratio * imagea.size[1])
                w2 = int(ratio * imagea.size[0])
            newsize = (w2, h2 )

            imageb = imagea.resize(newsize,self.algorithm)

            v = (int((self.size[0]-imageb.size[0])/2), int((self.size[1]-imageb.size[1])/2))
            new_im = Image.new("RGB", self.size) 
            new_im.paste(imageb, v)
            return {'image':new_im}       
        except:
            return {'image':image}


# ## ResizeWidget

# In[11]:


class ResizeWidget(BaseWidget):
    def __init__(self, action=None, parent=None, imageWidget=None, image=None):
        self.result        = {'image':image} 
        self.action       = action
        self.parent       = parent
        self.imageWidget  = imageWidget
                
        self.algorithmw = widgets.Dropdown(options=options,  description='algorithm', value = 0,style=style, layout=layout)
        self.algorithmw.observe(self.algorithmw_on_change,'value') 
        self.sizew = widgets.Dropdown(options=size_options,  description='sizes', style=style, layout=layout)
        self.sizew.observe(self.sizew_on_change,'value') 

        self.widthw  = widgets.Text(description = 'width',value = '640', style=style, layout=layout,disabled=False)
        self.heightw = widgets.Text(description = 'height',value = '480', style=style, layout=layout,disabled=False)
        self.update_button = widgets.Button(description='Resize', disabled=False, tooltip='resize',style=style, layout=layout)
        self.update_button.on_click(self.on_update_button_clicked)  
        
        self.vbox   = widgets.VBox([self.algorithmw,self.sizew,self.widthw,self.heightw,self.update_button])
        
        if action != None:
            self.widthw.value  = str(action.size[0])
            self.heightw.value = str(action.size[1])
            
        
    def algorithmw_on_change(self,change):
        if change['type'] == 'change' and change['name'] == 'value':
            self.update()

    def sizew_on_change(self,change):
        if change['type'] == 'change' and change['name'] == 'value':
            self.widthw.value  =  str(change['new'][0])        
            self.heightw.value =  str(change['new'][1])  
            self.update()
            
    def update(self):
        # get current image
        image = self.parent.result['image'] 
        
        if image != None:
            new_size = (int(self.widthw.value),int(self.heightw.value))
            self.action = ResizeAction(size=new_size,algorithm=self.algorithmw.value)
            self.result = self.action.evaluate(image=image)
            self.output_result(result=self.result)
            
    # the result: resized_image    
    def on_update_button_clicked(self,b):  
        self.update()


# ## FileWidget

# In[12]:


import ipywidgets as widgets
from IPython.display import display
vstyle = {'description_width': '250px'}
vlayout = {'width': '500px'}

class FileWidget(BaseWidget):
    def __init__(self, action=None, callback=None,get_save_action=None,  filename = 'videos.json', description = 'filename', child=None):
        self.data = None
        self.action = action
        self.callback = callback
        self.get_save_action = get_save_action
        
        self.filename = widgets.Text(description = description, value = filename, style=vstyle, layout=vlayout,disabled=False)    
        self.save_button = widgets.Button(description='Save', disabled=False, tooltip='reset all values',style=style)
        self.save_button.on_click(self.on_save_button_clicked)  
        self.load_button = widgets.Button(description='Load', disabled=False, tooltip='reset all values',style=style)
        self.load_button.on_click(self.on_load_button_clicked)  
        #self.delete_button = widgets.Button(description='Delete', disabled=False, tooltip='reset all values',style=style)
        #self.delete_button.on_click(self.on_video_delete_button_clicked)  
        self.hbox = widgets.HBox([self.filename,self.load_button,self.save_button])
        
        
    def _save(self,filename='actions.json', data=None):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)    

    def _load(self,filename='actions.json'):
            with open(filename, 'r') as fp:
                file = json.load(fp) 
                print(file)
                return file     

    def on_load_button_clicked(self,b): 
        filename = self.filename.value
        if self.action != None:
            self.action = self.action.load(filename=filename)
            self.callback(self.action)    
                
    def on_save_button_clicked(self,b):
        filename = self.filename.value 
        if self.get_save_action != None:
            action = self.get_save_action()
            action.save(filename=filename)
        elif self.action != None:
            self.action.save(filename=filename)
            print('on_save_button_clicked',filename)
            

    def video_clear_button_clicked(self,b):    
        self.video_url.value = ''
        self.video_title.value = ''
        self.video_location.value = ''
        
   
    def on_video_delete_button_clicked(self,b):
        del self.sources.videos[self.video_urls.value]


# In[13]:


#fw = FileWidget(filename = '../videos.json', description='pipeline')
#display(fw.hbox)


# ## ContrastBrightnessAction

# In[14]:


class ContrastBrightnessAction(BaseAction):
    def __init__(self, contrast=None, brightness=None):
        self.contrast = contrast
        self.brightness = brightness

    def change_contrast(self,img, level):
        factor = (259 * (level + 255)) / (255 * (259 - level))
        def contrast(c):
            return 128 + factor * (c - 128)
        return img.point(contrast)

    #constant by which each pixel is divided
    def change_brightness(self,im,constant=0.31):
        source = im.split()
        R, G, B = 0, 1, 2
        Red = source[R].point(lambda i: i/constant)
        Green = source[G].point(lambda i: i/constant)
        Blue = source[B].point(lambda i: i/constant)
        im = Image.merge(im.mode, (Red, Green, Blue))
        im.save('modified-image.jpeg', 'JPEG', quality=100)
        return im

    def getByteArray(self,image):    
        img_byte_arr = io.BytesIO()
        image.save(img_byte_arr, format='PNG')
        img_byte_arr = img_byte_arr.getvalue()
        return img_byte_arr

    def _enhance_image(self, im, contrast=0, brightness=1.0):
        im = self.change_contrast(im, contrast)
        im = self.change_brightness(im, brightness)
        return im

    def evaluate(self, image=None):
        _image = self._get_image(image)
        return {'image':self._enhance_image(_image, self.contrast, self.brightness)}
        
layout = {'width': '500px'}
style = {'description_width': '50px'}
    
class ContrastBrightnessWidget(BaseWidget):
    def __init__(self,action=None,imageWidget=None,layout=layout,style=style):
        self.action = action
        self.imageWidget  = imageWidget # None or used to display result
        self._image = None
                
        self.contrast_w    = widgets.IntSlider(value=0,description='contrast', min=-1000, max=1000, step=1,layout=layout)
        self.brightness_w  = widgets.FloatSlider(value=1.0,description='brightness', min=0, max=1.0, step=0.001, readout_format='.3f',layout=layout)
        #results_w = widgets.Text(description='results',value = '0', style=style, layout=layout)
        #image_w2      = widgets.Image(format='PNG')
        self.contrast_w.observe(self.on_value_change_enhance,'value')
        self.brightness_w.observe(self.on_value_change_enhance,'value')
        self.vbox   = widgets.VBox([self.contrast_w,self.brightness_w])
        #display(contrast_w,brightness_w,results_w,image_w2)

        if(self.action == None):
            self.action   = ContrastBrightnessAction(contrast=0,brightness=1.0)
        
    def on_value_change_enhance(self,change):
        self.action.contrast=self.contrast_w.value
        self.action.brightness=self.brightness_w.value        
        self.updateImage(update=True)
        
        #action._enhance_image(_image, contrast=contrast_w.value, brightness=brightness_w.value)
        #results_w.value = str(int(1))
        #image_w2.value = getByteArray(im)

    def evaluate(self,image=None):
        # Achtung schreibt image auch in ImageWidget !!!!!!! Doppelt
        self.updateImage(image=image)
        
        return self.result            
                
    # RESULT 
    # after image is updated 
    # called e.g. by DisplayWidget
    def updateImage(self,image=None,update=False):
        if(image != None):
            self._image = self.action._get_image(image)
        
        if(self._image != None):  
            
            self.result = self.action.evaluate(image=self._image)
            
            # write output
            if update == True:
                if(self.imageWidget != None):
                    self.imageWidget.evaluate(self.result['image'])  


# ## Video Action

# In[33]:


import pafy
import cv2
import time
import json
#from queue import Queue

class VideoAction(BaseAction):
    #def __init__(self, sources=None, id=None, index=0, skipframes=0):
    def __init__(self, sources=None, id=None, index=0, skipframes=0):    
        self.sources = sources
        self.id = id
        self.index = index
        self.skipframes = skipframes
        self._stream  = None
        self._capture = None
        self.video    = None
        
        self._last_state = {'id':self.id,'index':self.index}
        self.init(sources=sources)
        
    def dict(self):
        parameters = {'id':self.id,'index':self.index,'skipframes':self.skipframes}
        return {'class' : self.__class__.__name__, 'parameters' : parameters} 
        
    def init(self, sources=None, video=None):
        self.video = video
        
        if video == None and sources != None:
            try:
                self.sources = sources
                url = self.sources.videos[self.id]['url']   
                self.video = video = pafy.new(url)     
                return True
            except Exception as exception: 
                print('Exception VideoAction init', str(exception))    
                return False
            
        self.setIndex(self.index)
        return True
        
    # save video meta data    
    def saveVideo(self,filename='videos.json', videos=None):
        #print(videos)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(videos, f, ensure_ascii=False, indent=4)
        
    def loadVideo(self,filename='videos.json'):
        with open('videos.json', 'r') as fp:
            videos = json.load(fp)  
            return videos    
        
    # select stream   
    def _setIndex(self,index):
        if self.video != None:
            if index > len (self.video.streams) - 1:
                index = len (self.video.streams) - 1
        
        self.index = index
        self._last_state['index'] = index
        
        if self.video != None and self.index != None:
            #print(self.index, len(self.video.streams))
            self._stream = self.video.streams[self.index]
            self._capture = cv2.VideoCapture(self._stream.url)
            
    # deprecated        
    def setIndex(self,index):    
        self._setIndex(index)
        
    # TODO: check if index was updated
    # read next image of current stream     
    def evaluate(self, image=None):
        if self._last_state['index'] != self.index:
            self._setIndex(self.index)
        return {'image':self.readImage()}    
        
    # read next image of current stream    
    def readImage(self):
        if(self._capture == None):
            #print(self.index)
            self.setIndex(self.index)
        
        self.skipFrames(self.skipframes)
        grabbed, frame = self._capture.read()
        if grabbed == True:
            image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
            return image
        return None

    def readImages(self,number):
        images = []
        for idx in range(0, number):
            grabbed, frame = self._capture.read()
            image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
            images.append(image)

            for i in range(0, self.skipframes):
                grabbed, frame = self._capture.read()
                #print('skip',i)
        return images

    def skipFrames(self,skipframes=0):
        for i in range(0, skipframes):
                grabbed, frame = self._capture.read()
    
    def _readImageOfStream(self,stream):
        capture = cv2.VideoCapture(stream.url)
        grabbed, frame = capture.read()
        if grabbed == True:
            image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
            return image
        return None
    
    def getVstreams(self,indexes=None):
        global vstreams
        vstreams=[]
        for stream in self.video.streams:
            if indexes == None:
                image = self._readImageOfStream(stream)
                vstreams.append(image)
            else:
                vstreams.append(None) 

        if indexes != None:
            for index in indexes:
                image = self._readImageOfStream(video.streams[index])
                vstreams[index] = image
        return vstreams

    def getVstream(self,vstreams=None, index=None):
        if vstreams[index] == None:
            image = readImageOfStream(video.streams[index])
            vstreams[index] = image
        return vstreams[index]


# ## TemplateWrapperWidget

# In[16]:


layout2={'width': '350px'}

def addTemplateWrapperTab(_app,_class,action):
    _widget = _class(action=action, imageWidget=_app._imageWidget2, parent=_app._tab._widgets[len(_app._tab._widgets)-1])  
    _app._tab.add(_widget)
        
class TemplateWrapperWidget(BaseWidget):
    def __init__(self, action=None, imageWidget=None, parent=None):
        self.action = action
        self.parent = parent
        self.result = None
        self.imageWidget = imageWidget
        self.vbox   = widgets.VBox([])
            
    def evaluate(self, **kwargs):     
        self.result = self.action.evaluate(**kwargs)
        if isinstance(self.result, dict):
            _widgets = []
            for key, value in self.result.items():
                if key != 'image':
                    if isinstance(value,list):
                        value = len(value)
                    _widgets.append(widgets.Text(description = key, value=str(value), style=style, layout=layout2))
            self.vbox.children = _widgets
            
        return self.result


# # Sources

# ## LiveVideoSources

# In[17]:


import json
import pafy 

class LiveVideoSources(BaseAction):
    def __init__(self, filename='videos.json'):
        self.filename = filename
        
        self.videos = None
        self.load()
        
    def dict(self):
        return {'class' : self.__class__.__name__, 'parameters' : {'filename':self.filename}}     
        
    def get_keys(self):
        keys = {}
        for key, value in self.videos.items():
            keys[value['title']] = key
        return keys
        
    def load(self):
        with open(self.filename, 'r') as fp:
            self.videos = json.load(fp)  
            #return videos
            
    def save(self,filename=None):
        if filename == None:
            filename = self.filename
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.videos, f, ensure_ascii=False, indent=4)


# ## LiveVideoSourcesWidget

# In[18]:


import ipywidgets as widgets
from IPython.display import display
vstyle = {'description_width': '250px'}
vlayout = {'width': '500px'}

class LiveVideoSourcesWidget(BaseWidget):
    def __init__(self, action=None, imageWidget=None, child=None, filename = 'videos.json', id=None):
        self.sources = action  # to load/save meta data
        self.child = child
        self.action = None   # VideoAction - only for childs ????? obsolete ????
        #self.video = None    # to get meta data
        #self.id = None
        
        if(self.sources != None):
            filename = self.sources.filename
        
        self.video_filename = widgets.Text(description = 'filename',value = filename, style=vstyle, layout=vlayout,disabled=False)    
        self.video_save_button = widgets.Button(description='Save', disabled=False, tooltip='reset all values',style=style)
        self.video_save_button.on_click(self.on_video_save_button_clicked)  
        self.video_load_button = widgets.Button(description='Load', disabled=False, tooltip='reset all values',style=style)
        self.video_load_button.on_click(self.on_video_load_button_clicked)  
        self.video_delete_button = widgets.Button(description='Delete', disabled=False, tooltip='reset all values',style=style)
        self.video_delete_button.on_click(self.on_video_delete_button_clicked)  
        self.video_urls = widgets.Dropdown(description='videos', style=vstyle, layout=vlayout,disabled=False)
        self.video_urls.observe(self.video_urls_on_change)
       
        file_hbox = widgets.HBox([self.video_filename,self.video_load_button,self.video_save_button])
        video_urls_hbox = widgets.HBox([self.video_urls,self.video_delete_button])
        file_vbox =   widgets.VBox([file_hbox,video_urls_hbox])
        
        self.video_url = widgets.Text(description = 'url',value = '', style=vstyle, layout=vlayout,disabled=False)
        self.video_url.on_submit(self.on_value_submit_video_url)
        self.video_clear_button = widgets.Button(description='Clear', disabled=False, tooltip='reset all values',style=style)
        self.video_clear_button.on_click(self.video_clear_button_clicked)  
        video_url_hbox = widgets.HBox([self.video_url,self.video_clear_button])
        
        #video_streams = widgets.Text(description = 'streams',value = None, style=vstyle, layout=vlayout,disabled=True)
        #self.video_streams = widgets.Dropdown(description='streams', style=vstyle, layout=vlayout,disabled=True)
        self.video_title = widgets.Text(description = 'title',value = '', style=vstyle, layout=vlayout,disabled=False)
        self.video_location = widgets.Text(description = 'location',value = '', style=vstyle, layout=vlayout,disabled=False)
        self.video_country = widgets.Text(description = 'country',value = '', style=vstyle, layout=vlayout,disabled=False)
        self.video_objects = widgets.Text(description = 'objects',value = '', style=vstyle, layout=vlayout,disabled=False)
        
        self.vbox   = widgets.VBox([file_vbox, video_url_hbox, self.video_title,
                                     self.video_location,self.video_country,self.video_objects])

        # used e.g. to create widget for existing action
        if(self.sources != None):
            self.video_urls.options = self.sources.get_keys()
        if(self.sources != None and id != None):
            self.setVideoContainer(self.sources.videos[id])
            #print(self.sources.get_keys())
            #print(self.video_urls.options)
            self.video_urls.value = id
        
    def videoContainerTo_Dict(self):
        container = dict()
        container['url'] = self.video_url.value
        container['title'] = self.video_title.value
        container['location'] = self.video_location.value
        container['country'] = self.video_country.value
        container['objects'] = self.video_objects.value

        cropNormalizedArea = [0.0,0.0,1.0,1.0]    
        bookmark_dict = dict()
        bookmark_dict['reset'] = cropNormalizedArea
        container['bookmarks'] = bookmark_dict
        return container

    def setVideoContainer(self,container):
        self.video_url.value = container['url']
        self.video_title.value = container['title'] 
        self.video_location.value = container['location'] 
        self.video_country.value = container['country'] 
        self.video_objects.value = container['objects'] 

    # add new url, open video to get meta data, add container to sources / this should be moved to sources   
    def on_value_submit_video_url(self,change):
        self.add_new_video(change.value)
        
    def add_new_video(self,url):    
        video = pafy.new(url)
        self.video_title.value = video.title.rsplit('2021')[0].rstrip()
        container = self.videoContainerTo_Dict()
        self.sources.videos[video.videoid] = container
        #print(container)
        
    # VideoAction ändert sich nach Laden!!!! Es sollte nur eine geben ????? FOLLOWER
    # DisplayWidget / Player erzeugen neues Bild -> Pipeline oder update CropWidget
    # Flußgraph ?
    def init_video(self, id):
        #self.video = video = pafy.new(url)  #used also to init parameters
        
        if id is None:
            self.video_clear_button_clicked('')
            return
        
        self.setVideoContainer(self.sources.videos[id])

        # ????????????
        if self.child != None:
            #self.action = VideoAction(sources=self.sources,id=id,video=self.video)
            action = VideoAction(sources=self.sources, id=id)
            
            if action.video != None: 
                self.action = action
                #self.child.init(action=self.action, parent=self)
                self.child.init(action=self.action)
            else:
                self.video_clear_button_clicked('')
        
    def video_urls_on_change(self,change):
        #print('video_urls_on_change',change)
        if change['type'] == 'change' and change['name'] == 'value':
            id = change['new']
            #url = self.sources.videos[id]['url']  
            self.init_video(id)
            
    def video_clear_button_clicked(self,b):    
        self.video_url.value = ''
        self.video_title.value = ''
        self.video_location.value = ''
        
    def on_video_load_button_clicked(self,b): 
        filename = self.video_filename.value
        try:
            self.sources = LiveVideoSources(filename)
            self.video_urls.options = self.sources.get_keys()
        except Exception as e:  
            print('on_video_load_button_clicked:' + filename)
        
    def on_video_save_button_clicked(self,b):
        #global videos
        self.sources.filename = self.video_filename.value
        self.sources.save()  
        
    def on_video_delete_button_clicked(self,b):
        del self.sources.videos[self.video_urls.value]


# # Player

# ## HaltableActionPlayer (-> )

# In[19]:


import ipywidgets as widgets
import asyncio
import time

def deleteAllTasks(name):
    for task in asyncio.all_tasks(): 
                if task.get_name() == name: 
                    task.cancel()
             
            
class BooleanField(object):
    def __init__(self):
        value=False
            
class HaltableActionExecutor:

    def __init__(self, booleanField=None,imageWidget=None,sleep=0,action=None,name=None):
        self._checkbox = booleanField
        self.imageWidget = imageWidget
        self._sleep    = sleep
        self.action  = action
        self.name = name
        
    async def my_code(self):
        # This is your user code
        while True:
            if self._checkbox.value:
                self.imageWidget.evaluate(self.action.evaluate())
                #time.sleep(float(self._sleep.value))
            await asyncio.sleep(0.1)  # use this to temporarily give up control of the event loop to allow scheduling checkbox changes
    
    def start(self):
        task = asyncio.ensure_future(self.my_code())
        task.set_name(self.name)
        print(task)     


# # Application

# ## EasyVideoPipeLine

# In[20]:


def createActionOfDict(adict):
    return eval(adict['class'])(**adict['parameters'])

def evaluateActionDictList(_action_dict_list, _img):
    for action_dict in _action_dict_list:
        action = createActionOfDict(action_dict)
        _img = action.evaluate(_img)
    return _img
  
def saveActionDictList(filename='actions.json', actions=None):
        #print(videos)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(actions, f, ensure_ascii=False, indent=4)    

def loadActionDictList(filename='actions.json'):
        with open(filename, 'r') as fp:
            file = json.load(fp)  
            return file          

def evaluateActionList(action_list, img):
    for action in action_list:
        #print(type(action))
        img = action.evaluate(**img)
    return img

#CropAction(min=(0,0),max=(1,0.5),mode=1).evaluate(image=_action.readImage())['image']
#_actions = [CropAction(min=(0,0),max=(1,0.5),mode=1),ResizeAction(size=(900,500))]


# In[21]:


class EasyVideoPipeLine(BaseAction):
    def __init__(self, sources=None, actions=None):
        self.sources=sources
        self.actions=actions
       
        if sources != None and actions != None:
            pass
            #print (self.__dict__)
            #self.init()
        
    def dict(self):
        return {'class' : self.__class__.__name__, 'parameters' : self._create_parameters()}
    
    def _create_parameters(self):
        return {'sources' : self.sources.dict(), 'actions' : self._create_dicts(self.actions)}   
        
    def _create_dicts(self,actions):
        dicts = []
        for action in actions:
            if hasattr(action, 'dict'):
                dicts.append(action.dict())
            elif hasattr(action, 'action'):
                dicts.append(action.action.dict())  
            else:
                pass
                #print(self.__class__.__name__,'unknown')
        return dicts
    
    def _createActionOfDict(self,adict):
        return eval(adict['class'])(**adict['parameters'])
    
    def from_dict(self,adict):
        _new = self._createActionOfDict(adict)
        
        _new.sources = self._createActionOfDict(_new.sources)
        dicts = []
        for action in _new.actions:
            dicts.append( self._createActionOfDict(action))
        _new.actions = dicts
        _new.init()
        return _new
    
    def load(self,filename=None):
        _dict = super(EasyVideoPipeLine, self).load(filename=filename)
        return  self.from_dict(_dict)
    
    def init(self):
        self.actions[0].init(sources=self.sources)
        
    def evaluate(self, image=None):
        return evaluateActionList(self.actions, {})


# ## TabWidget

# In[22]:


class TabWidget(BaseAction):
    def __init__(self, action=None, displayWidget=None, imageWidget=None):
        self.action=action
        self.displayWidget = displayWidget
        self.imageWidget = imageWidget
        self.tab_idx = 0
        self._widgets = []
        self._widgets_vbox = []
        self.update_to = widgets.Text(description = 'update to',value = '', style=vstyle, layout=vlayout,disabled=False) 
        
        self._dict = {'CropAction':'CropWidget', 'ResizeAction':'ResizeWidget', 'LiveVideoSources':'LiveVideoSourcesWidget',
                     'ContrastBrightnessAction':'ContrastBrightnessWidget'}
        
        self.tab  = widgets.Tab(children = self._widgets_vbox)
        self.tab.observe(self.on_select, names='selected_index')  
        
    # setzt auch parent ??? Dann kann es aber keine Lücken geben. --> besser zunächst extern    
    def add(self,_class):    
        if _class.action != None:
            _name = _class.action.__class__.__name__
        else:    
            _name = _class.__class__.__name__
        #_class = eval(_dict[_name])(action=_action, imageWidget=_imageWidget2)
        self._widgets.append(_class)
        self._widgets_vbox.append(_class.vbox)
        self.tab.children = self._widgets_vbox  
        self.tab.set_title(len(self._widgets)-1, _name)
            
    def create_widget(self,action=None, imageWidget=None):
        if isinstance(action, dict):
            action = eval(action['class'])(**action['parameters'])
        
        _name = action.__class__.__name__
        if _name in self._dict.keys():
            _class = eval(self._dict[_name])(action=action, imageWidget=imageWidget)
            self.add(_class)
            return _class
        return None

    def evaluateWidgetActionList(self,image=None, start=1, end=None):
        if image == None:
            image = self.displayWidget.action.evaluate()
        if end == None:
            end = len(self._widgets)
        for index in range(start,end):
            image = self._widgets[index].evaluate(**image)
        return image

    # update interessant für Widget-Parameter
    # evaluate: berechnet image-Pipeline
    def on_select(self,widget):
        #     get the correct Output widget based on the index of the selected tab
        self.tab_idx = widget['new']  
        if self.update_to.value != '':
            end = int(self.update_to.value) + 1
        else :
            end = self.tab_idx+1
            
        #print ('tab end', end)    
        
        def _doit(image,update=False):
            #_imageWidget.evaluate(_crop.displayWidget.image)
            return self.imageWidget.evaluate(self.evaluateWidgetActionList(self.displayWidget.action.evaluate(), start=1, end=end))
        self.displayWidget.update = _doit
        #_doit(_widgets[0].evaluate())
        self.imageWidget.evaluate(self.evaluateWidgetActionList(self.displayWidget.action.evaluate(), start=1, end=end))    
        
    def evaluate(self, image=None):
        #image = self._get_image(image)    
        self.imageWidget.evaluate(self.evaluateWidgetActionList(self.displayWidget.action.evaluate(), start=1, end=self.tab_idx+1)) 


# ## SimpleApplicationWidget

# In[23]:


from IPython.display import display
from IPython.display import clear_output

class SimpleApplicationWidget(BaseWidget):
    def __init__(self, filename=None):
        self.pipeline = EasyVideoPipeLine()
        self.fileWidget = FileWidget(action=self.pipeline, callback=self.init, get_save_action=self.get_current_pipeline  ,filename = filename, description='pipeline')
        self._run = widgets.Checkbox(description='play',value=False)
        self.out  = widgets.Output()
        
        display(self.fileWidget.hbox,self.out)
        
        if filename != None:
            self.fileWidget.on_load_button_clicked('')
        
    def init(self,action):
        deleteAllTasks('test')
        self.pipeline = action
        self._displayWidget = DisplayWidget(action=self.pipeline.actions[0])
        self._imageWidget2 = ImageWidget(display=False)
        self._tab = TabWidget(action=self.pipeline.actions[0], displayWidget=self._displayWidget, imageWidget=self._imageWidget2)

        self._tab.create_widget(self.pipeline.sources,imageWidget=self._imageWidget2)

        _parent = None
        for _action in self.pipeline.actions:
            _widget = self._tab.create_widget(_action,imageWidget=self._imageWidget2)
            if _widget != None:
                _widget.parent = _parent
            _parent = _widget

        # !!!!!!!!!!!!!!! rework !!!!!!!    
        self._tab._widgets[1].crop_list.options = self.pipeline.actions[0].sources.videos[self.pipeline.actions[0].id]['bookmarks'].keys()    
        self._tab._widgets[1].parent = self.pipeline.actions[0]
        self._tab._widgets[1].displayWidget = self._displayWidget
        self._tab._widgets[0].child = self._tab._widgets[1]
        self._tab._widgets[0].action =  self.pipeline.actions[0]
        
        self._tab._widgets[1].displayWidget.action.sources = self._tab._widgets[0].sources
        
        self._tab._widgets[2].parent = self._tab._widgets[1]

        self.exe = HaltableActionExecutor(booleanField=self._run,imageWidget=self._imageWidget2,sleep=0,action=self._tab,name='test')
        self.exe.start()
        
        with self.out:
            clear_output()
            display(self._run,self._displayWidget.hbox, self._tab.tab)
            self._imageWidget2.display()
            display(self._tab.update_to) 

    def get_current_pipeline(self):
        return EasyVideoPipeLine(sources=self._tab._widgets[0].sources, actions=self._tab._widgets)

