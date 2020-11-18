EXT_CC_IP=$1
HOST_IP=$2

export PKGID=basic_1.0:5a294a12a1a89cd4eed3d4234fbc79f42eab2ac20cd176bc8ebbc07c597cd0ee

apk add curl

# Set hostname of external chaincode node
echo $EXT_CC_IP asset-transfer-basic.org1.example.com >> /etc/hosts
echo $HOST_IP peer0.org1.example.com peer0.org2.example.com orderer.example.com >> /etc/hosts

# Get chaincode description

curl https://mft.seeburger.de:443/portal-seefx/~public/MDI0Mjk4ZTQtZGQ3ZS00M2Y4LWIyMDktZjY1YzljN2MwMTlm?download > chaincode_description.tgz


CERT_BASE_PATH=/opt/gopath/src/github.com/hyperledger/fabric/peer

export CORE_PEER_TLS_ENABLED=true
export ORDERER_CA=${CERT_BASE_PATH}/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem
export PEER0_ORG1_CA=${CERT_BASE_PATH}/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt
export PEER0_ORG2_CA=${CERT_BASE_PATH}/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt
export PEER0_ORG3_CA=${CERT_BASE_PATH}/organizations/peerOrganizations/org3.example.com/peers/peer0.org3.example.com/tls/ca.crt


# Install for org1
export CORE_PEER_LOCALMSPID="Org1MSP"
export CORE_PEER_TLS_ROOTCERT_FILE=$PEER0_ORG1_CA
export CORE_PEER_MSPCONFIGPATH=${CERT_BASE_PATH}/organizations/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp
export CORE_PEER_ADDRESS=peer0.org1.example.com:7051

peer lifecycle chaincode install chaincode_description.tgz


# Install definition for org2
export CORE_PEER_LOCALMSPID="Org2MSP"
export CORE_PEER_TLS_ROOTCERT_FILE=$PEER0_ORG2_CA
export CORE_PEER_MSPCONFIGPATH=${CERT_BASE_PATH}/organizations/peerOrganizations/org2.example.com/users/Admin@org2.example.com/msp
export CORE_PEER_ADDRESS=peer0.org2.example.com:9051

peer lifecycle chaincode install chaincode_description.tgz


# Approve chaincode

peer lifecycle chaincode approveformyorg -o orderer.example.com:7050 --ordererTLSHostnameOverride orderer.example.com --tls --cafile $CERT_BASE_PATH/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem --channelID mychannel --name basic --version 1.0 --package-id $PKGID --sequence 1


export CORE_PEER_LOCALMSPID="Org1MSP"
export CORE_PEER_TLS_ROOTCERT_FILE=$PEER0_ORG1_CA
export CORE_PEER_MSPCONFIGPATH=${CERT_BASE_PATH}/organizations/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp
export CORE_PEER_ADDRESS=peer0.org1.example.com:7051

peer lifecycle chaincode approveformyorg -o orderer.example.com:7050 --ordererTLSHostnameOverride orderer.example.com --tls --cafile $CERT_BASE_PATH/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem --channelID mychannel --name basic --version 1.0 --package-id $PKGID --sequence 1
