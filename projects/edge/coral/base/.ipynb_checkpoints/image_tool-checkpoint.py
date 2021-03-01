import numpy as np
from PIL import Image
from imutils.video import FPS
from imutils.video import VideoStream

# TEXT
from PIL import Image, ImageFont, ImageDraw, ImageEnhance

class ImageTool:
    def __init__(self, filename=None, image=None, h=480):
        self.filename = filename
        self.image = image
        #print("ok")
        if image is None:
            self.image = ImageTool.load(filename)
        
        ratio = h / self.image.size[1]
        newsize = (int(ratio * self.image.size[0]), int(ratio * self.image.size[1]) )
        #print(self.image.size, newsize)
        #image = image.thumbnail(newsize, Image.ANTIALIAS)
        self.image = self.image.resize([newsize[0],newsize[1]],Image.ANTIALIAS)
        #print(self.image.size)
        
    def testload(self):
        #load(filename)
        print(self.image.size)
        display(self.image) 
        #return image
        
    def printSize(self):
        print(self.image.size)
        
    def load(filename):
        return Image.open(open(filename, 'rb'))
    
    def crop(self,left, top, right, bottom):  
        try:
            #left, top, right, bottom = 0,0,640,480
            #left, top, right, bottom = 851-640,0,851,480
            image = self.image.crop((left, top, right, bottom))
            #print(image.size)
            self.image = image
        except IOError:
            print( "cannot create thumbnail for '%s'" % filename)
 
    def drawText(self, text, x,y, size=20, color='red'):
        draw = ImageDraw.Draw(self.image)
        #draw.rectangle(((0, 00), (100, 100)), fill="black")
        fnt = ImageFont.truetype("DejaVuSans.ttf", size)
        #draw.text((x,y), text, font=fnt, fill=(255,255,255,128))
        draw.text((x,y), text, font=fnt, fill=color)
        #!ls /usr/share/fonts/truetype/dejavu 

    def drawPoint(self, x,y,r,rgba):
        draw = ImageDraw.Draw(self.image)
        leftUpPoint = (x-r, y-r)
        rightDownPoint = (x+r, y+r)
        twoPointList = [leftUpPoint, rightDownPoint]
        draw.ellipse(twoPointList, fill=rgba)

    def drawLine(self,ax,ay,bx,by,color='red',width=1):
        draw = ImageDraw.Draw(self.image)
        draw.line((ax,ay,bx,by), fill=color, width=width)

    def drawBox(self,gx0 , gy0, gx1, gy1,color='white',width=1):
        draw = ImageDraw.Draw(self.image)
        draw.rectangle(((gx0, gy0), (gx1, gy1)), outline=color, width=width)
        
## video class
import time
from IPython.display import display, clear_output
from PIL import ImageChops

class videoTool:
    def __init__(self,width=640,height=480):
        self.image = None
        #self.vs = None
        
        # Initialize video stream
        self.vs = VideoStream(usePiCamera=False, resolution=(width, height)).start()
        time.sleep(1)
        self.fps = FPS().start()
        self.screenshot = self.vs.read()
        self.image = Image.fromarray(self.screenshot)
    
    def loop(self):
        while True:
            screenshot = self.vs.read()
            self.image = Image.fromarray(screenshot)
            clear_output(wait=True)
            display(self.image)
         
    
    def loop1(self):
        screenshot = self.vs.read()
        image1 = Image.fromarray(screenshot)
        display(image1)
    
        while True:
            screenshot = self.vs.read()
            self.image = Image.fromarray(screenshot)
            diff = ImageChops.difference(image1, self.image)
         
            if diff.getbbox():
                clear_output(wait=True)
                display(self.image)
                #print("images are different")
                image1 = self.image
            else:
                print("images are the same")   
    
    def stop(self):
        self.fps.stop()
        self.vs.stop()
## video    
    
    
def fromVideo(filepath):
    # Importing all necessary libraries 
    import cv2 
    import os 
    outputpath = "./data2"  

    # Read the video from specified path 
    cam = cv2.VideoCapture("seewasen.mp4") 
  
    try: 
        # creating a folder named data 
        if not os.path.exists(outputpath): 
            os.makedirs(outputpath) 
  
    # if not created then raise error 
    except OSError: 
        print ('Error: Creating directory of data') 
  
    # frame 
    currentframe = 0
  
    while(True): 
      
        # reading from frame 
        ret,frame = cam.read() 
  
        if ret: 
            # if video is still left continue creating images 
            name = outputpath + '/frame_' + str(format(currentframe, '05d')) + '.jpg'
            #print ('Creating...' + name) 
  
            # writing the extracted images 
            cv2.imwrite(name, frame) 
  
            # increasing counter so that it will 
            # show how many frames are created 
            currentframe += 1
        else: 
            break
  
    # Release all space and windows once done 
    cam.release() 
    cv2.destroyAllWindows()     