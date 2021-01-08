CHANNEL_NAME=$2
ORDERER_HOST=orderer.example.com
ORDERER_PORT=7050
ORDERER_IP=$1

echo $ORDERER_IP orderer.example.com >> /etc/hosts

export FABRIC_CFG_PATH=/opt/scray/conf/configtx

cd /tmp/
mkdir ./channel-artifacts/
configtxgen -profile TwoOrgsChannel -outputCreateChannelTx ./channel-artifacts/$CHANNEL_NAME.tx -channelID $CHANNEL_NAME 

# Get orderer admin config
export CORE_PEER_LOCALMSPID="Org1MSP"
export CORE_PEER_TLS_ROOTCERT_FILE=/opt/gopath/src/github.com/hyperledger/fabric/peer/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt
export CORE_PEER_MSPCONFIGPATH=/opt/gopath/src/github.com/hyperledger/fabric/peer/organizations/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp
export ORDERER_CA=/opt/gopath/src/github.com/hyperledger/fabric/peer/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem
export FABRIC_CFG_PATH=/etc/hyperledger/fabric

peer channel create -o $ORDERER_HOST:$ORDERER_PORT  --ordererTLSHostnameOverride  $ORDERER_HOST -c $CHANNEL_NAME -f ./channel-artifacts/$CHANNEL_NAME.tx --outputBlock ./channel-artifacts/$CHANNEL_NAME.block --tls --cafile $ORDERER_CA
