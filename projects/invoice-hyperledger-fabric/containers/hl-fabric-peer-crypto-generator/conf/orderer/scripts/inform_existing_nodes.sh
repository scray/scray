
ORDERER_IP=$1
CHANNEL_NAME=$2 #mychannel
NEW_ORG_NAME=$3
	
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_LOCALMSPID="AdminOrgMSP"
export CORE_PEER_TLS_ROOTCERT_FILE=/mnt/conf/admin/organizations/peerOrganizations/kubernetes.research.dev.seeburger.de/peers/peer0.kubernetes.research.dev.seeburger.de/msp/cacerts/ca.kubernetes.research.dev.seeburger.de-cert.pem 
export CORE_PEER_MSPCONFIGPATH=/mnt/conf/admin/organizations/peerOrganizations/kubernetes.research.dev.seeburger.de/users/Admin\@kubernetes.research.dev.seeburger.de/msp/
export ORDERER_CA=/mnt/conf/orderer/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem #Fixme use orderer name


echo $ORDERER_IP orderer.example.com >> /etc/hosts

# Export existing channel configuration
peer channel fetch config config_block.pb -o orderer.example.com:7050 -c $CHANNEL_NAME --tls --cafile $ORDERER_CA
configtxlator proto_decode --input config_block.pb --type common.Block | jq .data.data[0].payload.data.config > config.json

# Upload CA cert
SHARED_FS_HOST=10.15.136.41:30080
SHARED_FS_USER=scray
SHARED_FS_PW=scray
apk add curl
curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/ca
curl --user $SHARED_FS_USER:$SHARED_FS_PW -T $ORDERER_CA http://$SHARED_FS_HOST/ca/tlsca.example.com-cert.pem

# Get configuration of new peer
curl --user 'scray:scray' http://${SHARED_FS_HOST}/newmemberrequests/$CHANNEL_NAME/${NEW_ORG_NAME}.json > new_member_org.json


# Add org3 data to existing config
jq -s ".[0] "*" {\"channel_group\":{\"groups\":{\"Application\":{\"groups\": {\"${NEW_ORG_NAME}MSP\":.[1]}}}}}" config.json ./new_member_org.json > modified_config.json

configtxlator proto_encode --input config.json --type common.Config --output config.pb
configtxlator proto_encode --input modified_config.json --type common.Config --output modified_config.pb
configtxlator compute_update --channel_id $CHANNEL_NAME --original config.pb --updated modified_config.pb --output org3_update.pb
configtxlator proto_decode --input org3_update.pb --type common.ConfigUpdate | jq . > org3_update.json
echo '{"payload":{"header":{"channel_header":{"channel_id":"'$CHANNEL_NAME'", "type":2}},"data":{"config_update":'$(cat org3_update.json)'}}}' | jq . > org3_update_in_envelope.json
configtxlator proto_encode --input org3_update_in_envelope.json --type common.Envelope --output org3_update_in_envelope.pb

# Sign update by first admin
peer channel signconfigtx -f org3_update_in_envelope.pb

peer channel update -f org3_update_in_envelope.pb -c $CHANNEL_NAME -o orderer.example.com:7050 --tls --cafile $ORDERER_CA
