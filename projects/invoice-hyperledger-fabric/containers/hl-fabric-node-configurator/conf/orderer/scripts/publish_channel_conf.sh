CHANNEL_NAME=$1
SHARED_FS_HOST=$2

export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_LOCALMSPID="AdminOrgMSP"
export CORE_PEER_TLS_ROOTCERT_FILE=/mnt/conf/admin/organizations/peerOrganizations/kubernetes.research.dev.seeburger.de/peers/peer0.kubernetes.research.dev.seeburger.de/msp/cacerts/ca.kuber
export CORE_PEER_MSPCONFIGPATH=/mnt/conf/admin/organizations/peerOrganizations/kubernetes.research.dev.seeburger.de/users/Admin\@kubernetes.research.dev.seeburger.de/msp/
export ORDERER_CA=/mnt/conf/orderer/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem #Fixme use orderer name


# Export existing channel configuration
peer channel fetch config config_block.pb -o orderer.example.com:7050 -c $CHANNEL_NAME --tls --cafile $ORDERER_CA
configtxlator proto_decode --input config_block.pb --type common.Block > config.json

# Upload CA cert
SHARED_FS_USER=scray
SHARED_FS_PW=scray
curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/channel/
curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/channel/configuration
curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/channel/configuration/$CHANNEL_NAME
curl --user $SHARED_FS_USER:$SHARED_FS_PW -T config.json http://$SHARED_FS_HOST/channel/configuration/$CHANNEL_NAME/config.json
