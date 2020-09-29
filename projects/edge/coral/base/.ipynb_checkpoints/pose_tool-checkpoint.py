#import pose_camera
from PIL import Image, ImageDraw, ImageFont
import random
colors = [(255,0,0,255) ,(0,255,0,255) ,(0,0,255,255),(255,255,0,255) ,(0,255,255,255) ,(255,255,255,255), \
         (125,0,0,255) ,(125,255,0,255) ,(125,0,255,255),(125,255,0,255) ,(125,255,255,255) ,(125,255,255,255)]


        
def get_xys(pose, min_score=-1):
    xys = {}
    for label, keypoint in pose.keypoints.items():
        score =  float(keypoint.score)
        if score < min_score: continue
        kp_y = int((keypoint.yx[0] ) )
        kp_x = int((keypoint.yx[1] ) )
        xys[label] = (kp_x, kp_y, score)
        #print(' %-20s x=%-4d y=%-4d score=%.1f' %
        #          (label, keypoint.yx[1], keypoint.yx[0], keypoint.score))
    return xys        
        
def drawPoint(image,x,y,r,rgba):
    draw = ImageDraw.Draw(image)
    leftUpPoint = (x-r, y-r)
    rightDownPoint = (x+r, y+r)
    twoPointList = [leftUpPoint, rightDownPoint]
    draw.ellipse(twoPointList, fill=rgba)

def drawPosePoints(image,poses,EDGES):    
    for pose in poses:
        #if pose.score < 0.4: continue
        rgba =  random.choice(colors)     
        #print('\nPose Score: ', pose.score)
        xys = {}
        minScoreKeypoint = 0.9
        for label, keypoint in pose.keypoints.items():
            if keypoint.score < minScoreKeypoint : continue
            drawPoint(image,keypoint.yx[1], keypoint.yx[0],2,rgba)
            kp_y = int((keypoint.yx[0] ) )
            kp_x = int((keypoint.yx[1] ) )

            xys[label] = (kp_x, kp_y)
            #print(' %-20s x=%-4d y=%-4d score=%.1f' %
            #      (label, keypoint.yx[1], keypoint.yx[0], keypoint.score))
        #print(xys)
    
        for a, b in EDGES:
            if a not in xys or b not in xys: continue
            ax, ay = xys[a]
            bx, by = xys[b]
            #dwg.add(dwg.line(start=(ax, ay), end=(bx, by), stroke=color, stroke_width=2))
            draw = ImageDraw.Draw(image)
            draw.line((ax,ay,bx,by), fill=128, width=3)
            
def printPoses(poses, mninScorePose=-0.1, minScoreKeypoint=-0.1):
    for pose in poses:
        if pose.score < mninScorePose : continue
        print('\nPose Score: ', pose.score)
        for label, keypoint in pose.keypoints.items():
            #drawPoint(keypoint.yx[1], keypoint.yx[0],2,rgba)
            if keypoint.score < minScoreKeypoint : continue
            print(' %-20s x=%-4d y=%-4d score=%.2f' %
                (label, keypoint.yx[1], keypoint.yx[0], keypoint.score))
            
class Pose:
    def __init__(self, pose, minScoreKeypoint=-1):
        self.pose = pose
        self.xys = get_xys(pose, minScoreKeypoint)
        
    def addPointsText(self,img,keys,color,r,fontsize):
        for key in keys:
            if key in self.xys:
                #print(xys[key])
                x = self.xys[key][0]
                y = self.xys[key][1]
                img.drawPoint( x,y,r,color)
                img.drawText(str(key), x,y,fontsize, color=color)
                
    def addSkeleton(self,img,EDGES,color='red',width=1):
        for a, b in EDGES:
            if a not in self.xys or b not in self.xys: continue
            ax, ay = self.xys[a][0],self.xys[a][1]
            bx, by = self.xys[b][0],self.xys[b][1]
            #dwg.add(dwg.line(start=(ax, ay), end=(bx, by), stroke=color, stroke_width=2))
            #draw = ImageDraw.Draw(pil_image)
            #draw.line((ax,ay,bx,by), fill="white", width=5)
            #print((ax,ay,bx,by))
            img.drawLine(ax,ay,bx,by,color=color,width=width)
                
    def printPose(self,mninScorePose=-0.1, minScoreKeypoint=-0.1):
        #if self.pose.score < mninScorePose : continue
        print('\nPose Score: ', self.pose.score)
        for label, keypoint in self.pose.keypoints.items():
            #drawPoint(keypoint.yx[1], keypoint.yx[0],2,rgba)
            if keypoint.score < minScoreKeypoint : continue
            print(' %-20s x=%-4d y=%-4d score=%.2f' %
                (label, keypoint.yx[1], keypoint.yx[0], keypoint.score))