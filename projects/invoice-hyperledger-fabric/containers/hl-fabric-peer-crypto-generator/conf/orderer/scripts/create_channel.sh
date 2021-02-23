CHANNEL_NAME=$1
ORDERER_HOST=$2
ORDERER_PORT=$3

export FABRIC_CFG_PATH=/mnt/conf/orderer/

cd /mnt/conf/orderer/
configtxgen -profile TwoOrgsChannel -outputCreateChannelTx ./channel-artifacts/${CHANNEL_NAME}.tx -channelID $CHANNEL_NAME
export FABRIC_CFG_PATH=/mnt/conf/


export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_LOCALMSPID="AdminOrgMSP"
export CORE_PEER_TLS_ROOTCERT_FILE=/mnt/conf/admin/organizations/peerOrganizations/kubernetes.research.dev.seeburger.de/peers/peer0.kubernetes.research.dev.seeburger.de/msp/cacerts/ca.kubernetes.research.dev.seeburger.de-cert.pem 
export CORE_PEER_MSPCONFIGPATH=/mnt/conf/admin/organizations/peerOrganizations/kubernetes.research.dev.seeburger.de/users/Admin\@kubernetes.research.dev.seeburger.de/msp/



export CORE_PEER_ADDRESS=$ORDERER_HOST:$ORDERER_PORT


peer channel create -o $CORE_PEER_ADDRESS  --ordererTLSHostnameOverride orderer.example.com -c $CHANNEL_NAME -f ./channel-artifacts/${CHANNEL_NAME}.tx --outputBlock ./channel-artifacts/channel1.block --tls --cafile ${PWD}/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem

#configtxlator proto_decode --input genesis.block --type common.Block --output config_block.json
