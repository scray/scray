#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import subprocess
import json

##############################################
# different ways  to execute commands
 
# in Kubernetes
def executeKubectlCmd(cmd):
    try:    
        output  = subprocess.check_output(['/home/jovyan/work/usr/bin/kubectl'] + cmd)
        return output.decode('ascii')
    except Exception as e:
        return str(e)       
    
# at the peer    
def executePeerCmd(pod_name,cmd):
    try:  
        cmd1 = "source /root/.bashrc && "
        output  = subprocess.check_output(['/home/jovyan/work/usr/bin/kubectl', 'exec','-it', pod_name, '-c', 'scray-peer-cli','--', '/bin/bash','-c',cmd1 + cmd])
        return output.decode('ascii')
    except Exception as e:
        return str(e)

def executePeerCmd2(pod_name,cmd):
    try:  
        cmd1 = "source /root/.bashrc && "
        output  = subprocess.run(['/home/jovyan/work/usr/bin/kubectl', 'exec','-it', pod_name, '-c', 'scray-peer-cli','--', '/bin/bash','-c',cmd1 + cmd], stdout = subprocess.PIPE, stderr=subprocess.PIPE)
        return output
    except Exception as e:
        return str(e)    
    
# in Jupyter    
def executeLocalCmd(cmd):
    try:  
        output  = subprocess.check_output(cmd)
        return output
    except Exception as e:
        return str(e)    

    
def toCmd(strlist):
    return ''.join(str(e + ' ') for e in strlist)[:-1]
    
##############################################
## peer

def getPort(peername='', name='peer-gossip'):
    callopt= 'jsonpath=\"{.spec.ports[?(@.name==\'' + name + '\')].nodePort}\"'
    try:    
        callProcess  = subprocess.check_output(['/home/jovyan/work/usr/bin/kubectl', 'get','service',peername,'-o', callopt])
        return str(callProcess)[3:-2]
    except Exception as e:
        return str(e)

def createConfig(peername='',peer_listen_port='',peer_gossip_port=''):
    try:  
        output  = subprocess.check_output(['/home/jovyan/work/usr/bin/kubectl', 'create','configmap','hl-fabric-peer-' + peername,
                                          '--from-literal=hostname=kubernetes.research.dev.seeburger.de',
                                          '--from-literal=org_name=' + peername,
                                          '--from-literal=CORE_PEER_ADDRESS=kubernetes.research.dev.seeburger.de:' + peer_listen_port,
                                          '--from-literal=CORE_PEER_GOSSIP_EXTERNALENDPOINT=kubernetes.research.dev.seeburger.de:' + peer_gossip_port,
                                          '--from-literal=CORE_PEER_LOCALMSPID=' + peername + 'MSP'])
        return str(output)
    except Exception as e:
        return str(e)  

#!/home/jovyan/work/usr/bin/kubectl get configmap -o json     
def getConfigmap(): 
    return json.loads(executeKubectlCmd(['get','configmap', '-o', 'json']).decode('ascii'))
       
def getPod(peername):
    try:    
        callProcess  = subprocess.check_output(['/home/jovyan/work/usr/bin/kubectl', 'get','pod','-l','app=' + peername,'-o', 
                                                'jsonpath=\"{.items[0].metadata.name}\"'])
        return str(callProcess)[3:-2]
    except Exception as e:
        return str(e)     
    
def getPods():
    return json.loads(executeKubectlCmd(['get', 'pods', '-o', 'json']))       


# In[ ]:




