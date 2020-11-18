EXT_CC_IP=$1
HOST_IP=$2

export PKGID=basic_1.0:5a294a12a1a89cd4eed3d4234fbc79f42eab2ac20cd176bc8ebbc07c597cd0ee

CERT_BASE_PATH=/opt/gopath/src/github.com/hyperledger/fabric/peer

export CORE_PEER_TLS_ENABLED=true
export ORDERER_CA=${CERT_BASE_PATH}/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem
export PEER0_ORG1_CA=${CERT_BASE_PATH}/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt
export PEER0_ORG2_CA=${CERT_BASE_PATH}/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt

export CORE_PEER_LOCALMSPID="Org1MSP"
export CORE_PEER_TLS_ROOTCERT_FILE=$PEER0_ORG1_CA
export CORE_PEER_MSPCONFIGPATH=${CERT_BASE_PATH}/organizations/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp
export CORE_PEER_ADDRESS=peer0.org1.example.com:7051

peer lifecycle chaincode commit -o orderer.example.com:7050 --ordererTLSHostnameOverride orderer.example.com --tls --cafile $CERT_BASE_PATH/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem --channelID mychannel --name basic --peerAddresses peer0.org1.example.com:7051 --tlsRootCertFiles $CERT_BASE_PATH/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt --peerAddresses peer0.org2.example.com:9051 --tlsRootCertFiles ${CERT_BASE_PATH}/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt --version 1.0 --sequence 1

export CORE_PEER_LOCALMSPID="Org1MSP"
export CORE_PEER_TLS_ROOTCERT_FILE=$PEER0_ORG1_CA
export CORE_PEER_MSPCONFIGPATH=${CERT_BASE_PATH}/organizations/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp
export CORE_PEER_ADDRESS=peer0.org1.example.com:7051

peer chaincode invoke -o orderer.example.com:7050 --ordererTLSHostnameOverride orderer.example.com --tls --cafile ${CERT_BASE_PATH}/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem -C mychannel -n basic --peerAddresses peer0.org1.example.com:7051 --tlsRootCertFiles ${CERT_BASE_PATH}/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt --peerAddresses peer0.org2.example.com:9051 --tlsRootCertFiles ${CERT_BASE_PATH}/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt -c '{"function":"InitLedger","Args":[]}'
