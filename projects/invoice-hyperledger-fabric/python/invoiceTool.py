import os
import json
import subprocess
import pandas as pd

pwd0 = "/home/jovyan/work/fabric-samples/"
pwd = "/home/jovyan/work/fabric-samples/test-network"

os.environ['PATH'] = "/home/jovyan/work/fabric-samples/bin:" + str(os.environ.get('PATH')) 
os.environ['CORE_PEER_TLS_ENABLED'] = "true"
os.environ['CORE_PEER_LOCALMSPID'] = "Org1MSP"
os.environ['CORE_PEER_TLS_ROOTCERT_FILE'] = pwd + "/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt"
os.environ['CORE_PEER_MSPCONFIGPATH'] = pwd + "/organizations/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp"
os.environ['CORE_PEER_ADDRESS'] = "peer0.org1.example.com:7051"
os.environ['FABRIC_CFG_PATH'] = pwd0 + "/config/"

addr0 = 'peer0.org1.example.com:7050'
orderer = 'orderer.example.com'
ordererpem = pwd + '/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem'
addr1 = 'peer0.org1.example.com:7051'
crt1 = pwd + '/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt'
addr2 = 'peer0.org2.example.com:9051'
crt2 = pwd + '/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt' 

class peer():
    def __init__(self, addr = '', crt = '', pem = ''):
        self.addr = addr
        self.crt = crt
        self.pem = pem
    def getCertificate(self):
        try:
            return str(subprocess.check_output(['cat', self.crt]))[2:-3]   
        except Exception as e:
            return str(e)
        
def createInvoice(id=19,invoiceNumber="123433",vat=0,netto=0,countryOrigin='',countryReceiver='',received="false",sell="false",hash=0):
    key = 'CAR' + str(id) 
    callopt = '{\"Args\":[\"createInvoice\",' + '\"' + key + '\",' + '\"' + invoiceNumber + '\",' + '\"' + str(vat) + '\",' + '\"' + str(netto) + '\",' + '\"' + str(countryOrigin) + '\",' + '\"' + str(countryReceiver) + '\",' +'\"' + received + '\",' + '\"' + sell + '\"' + ']}'
    
    try:
        return subprocess.check_output(['peer', 'chaincode', 'invoke', 
            '-o', addr0, '--ordererTLSHostnameOverride', orderer, '--tls', '--cafile', ordererpem, '-C', 'mychannel', 
            '-n', 'scray-invoice-example', 
            '--peerAddresses', addr1, '--tlsRootCertFiles', crt1,
            '--peerAddresses', addr2, '--tlsRootCertFiles', crt2,
            '-c',callopt])   
    except Exception as e:
        return str(e)
        
        
def queryInvoice(id=19):
    key = 'CAR' + str(id) 
    callopt= '{\"Args\":[\"queryInvoice\",' + '\"' + key + '\"' + ']}'
    try:    
        callProcess  = subprocess.check_output(['peer', 'chaincode','query', '-C', 'mychannel', '-n', 'scray-invoice-example', '-c',callopt])
        return json.loads(str(callProcess)[2:-3])
    except:
        return None

def getInvoices():
    callopt= '{\"Args\":[\"queryAllInvoices\"'  + ']}'
    try:
        callProcess  = subprocess.check_output(['peer', 'chaincode','query', '-C', 'mychannel', '-n', 'scray-invoice-example', '-c',callopt])
        return json.loads(str(callProcess)[2:-3])
    except:
        callProcess = ''
        
def getInvoicesAsDF():
    return pd.json_normalize(getInvoices())
        
def markAsReceived(id):
    key = 'CAR' + str(id) 
    callopt= '{\"Args\":[\"markAsReceived\",' + '\"' + key + '\"' + ']}'
    try:
        return subprocess.check_output(['peer', 'chaincode', 'invoke', 
            '-o', addr0, '--ordererTLSHostnameOverride', orderer, '--tls', '--cafile', ordererpem, '-C', 'mychannel', 
            '-n', 'scray-invoice-example', 
            '--peerAddresses', addr1, '--tlsRootCertFiles', crt1,
            '--peerAddresses', addr2, '--tlsRootCertFiles', crt2,
            '-c',callopt])   
    except Exception as e:
        return str(e)
        

def markOrderReceived(id):
    key = 'CAR' + str(id) 
    callopt= '{\"Args\":[\"markOrderReceived\",' + '\"' + key + '\"' + ']}'
    try:
        return subprocess.check_output(['peer', 'chaincode', 'invoke', 
            '-o', addr0, '--ordererTLSHostnameOverride', orderer, '--tls', '--cafile', ordererpem, '-C', 'mychannel', 
            '-n', 'scray-invoice-example', 
            '--peerAddresses', addr1, '--tlsRootCertFiles', crt1,
            '--peerAddresses', addr2, '--tlsRootCertFiles', crt2,
            '-c',callopt])   
    except Exception as e:
        return str(e)
    
def getinfo():
    try:    
        callProcess  = subprocess.check_output(['peer', 'channel','-c', 'mychannel', 'getinfo'])
        return json.loads(str(callProcess)[19:-3])
    except Exception as e:
        return str(e)