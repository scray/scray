#!/usr/bin/env python
# coding: utf-8

# In[1]:


import subprocess
import json

##############################################
# different ways  to execute commands
 
# in Kubernetes
def executeKubectlCmd(cmd, decode='ascii'):
    try:    
        output  = subprocess.check_output(['/home/jovyan/work/usr/bin/kubectl'] + cmd)
        if decode == 'json':
            return json.loads(output.decode('ascii'))
        else:
            return output.decode(decode)
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

def executeLocalCmd2(cmd):
    try:  
        output  = subprocess.run(cmd, stdout = subprocess.PIPE, stderr=subprocess.PIPE)
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

def deleteConfig(peername=''):
    try:  
        output  = subprocess.check_output(['/home/jovyan/work/usr/bin/kubectl', 'delete','configmap','hl-fabric-peer-' + peername])
        return str(output)
    except Exception as e:
        return str(e)  
        
    
def createConfig(peername='',peer_listen_port='',peer_gossip_port=''):
    try:  
        output  = subprocess.check_output(['/home/jovyan/work/usr/bin/kubectl', 'create','configmap','hl-fabric-peer-' + peername,
                                          '--from-literal=hostname=kubernetes.research.dev.seeburger.de',
                                          '--from-literal=org_name=' + peername,
                                          '--from-literal=data_share=hl-fabric-data-share-service:80',
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


#getPods()['metadata'].keys()

#getPods().keys()
getPods()['items'][0]['metadata'].keys()
getPods()['items'][0]['metadata']['name']

for item in getPods()['items']:
    print(item['metadata']['name'])
    
  
deployments = json.loads(executeKubectlCmd(['get', 'deployments', '-o', 'json'])) 


# In[ ]:


deployments.keys()

for item in deployments['items']:
    print(item['metadata']['name'])
    
    
services = json.loads(executeKubectlCmd(['get', 'services', '-o', 'json']))    

for item in services['items']:
    print(item['metadata']['name'])


# In[ ]:


type(executeKubectlCmd(['get', 'service', 'orderer-org1-scray-org']))


# In[ ]:


services['items'][2]['spec']['ports']


# In[ ]:


get_ipython().system('/home/jovyan/work/usr/bin/kubectl get service orderer-org1-scray-org -o jsonpath="{.spec.ports[?(@.name==\'orderer-listen\')].nodePort}"')


# In[ ]:


getPort(peername='orderer-org1-scray-org',name='orderer-listen')


# In[ ]:


getPod('orderer-org1-scray-org')


# In[ ]:


get_ipython().system("/home/jovyan/work/usr/bin/kubectl exec --stdin --tty 'orderer-org1-scray-org-5f97c57d44-qrz4b' -c scray-orderer-cli  -- /bin/sh /mnt/conf/orderer/scripts/create_channel.sh 'super' orderer.example.com '7050'")


# In[ ]:


getPort(peername='olya0', name='peer-gossip'), getPort(peername='olya0', name='peer-listen')

