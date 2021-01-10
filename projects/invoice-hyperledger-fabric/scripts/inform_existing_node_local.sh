ORDERER_IP=$1
CHANNEL_NAME=$2 #mychannel
NEW_ORG_NAME=$3
FABRIC_LOCATION=$4
WORK_LOCATION=$5
SHARED_FS_HOST=10.15.136.41:30080
SHARED_FS_USER=scray
SHARED_FS_PW=scray

export CORE_PEER_LOCALMSPID="Org1MSP"
export CORE_PEER_TLS_ROOTCERT_FILE=${FABRIC_LOCATION}/organizations/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/tls/ca.crt
export CORE_PEER_MSPCONFIGPATH=${FABRIC_LOCATION}/organizations/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp
export CORE_PEER_ADDRESS=peer0.org1.example.com:7051
export ORDERER_CA=${FABRIC_LOCATION}/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem

#echo $ORDERER_IP orderer.example.com >> /etc/hosts

# Export existing channel configuration
peer channel fetch config  $WORK_LOCATION/config_block.pb -o orderer.example.com:7050 -c $CHANNEL_NAME --tls --cafile $ORDERER_CA 
configtxlator proto_decode --input $WORK_LOCATION/config_block.pb --type common.Block | jq .data.data[0].payload.data.config > $WORK_LOCATION/config.json

# Upload CA cert
#apk add curl
curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/ca
curl --user $SHARED_FS_USER:$SHARED_FS_PW -T ${FABRIC_LOCATION}/organizations/ordererOrganizations/example.com/orderers/orderer.example.com/msp/tlscacerts/tlsca.example.com-cert.pem http://$SHARED_FS_HOST/ca/tlsca.example.com-cert.pem

# Get configuration of new peer
curl --user 'scray:scray' http://${SHARED_FS_HOST}/newmemberrequests/$CHANNEL_NAME/${NEW_ORG_NAME}.json > $WORK_LOCATION/new_member_org.json


# Add org3 data to existing config
jq -s ".[0] "*" {\"channel_group\":{\"groups\":{\"Application\":{\"groups\": {\"${NEW_ORG_NAME}MSP\":.[1]}}}}}" $WORK_LOCATION/config.json $WORK_LOCATION/new_member_org.json > $WORK_LOCATION/modified_config.json

configtxlator proto_encode --input $WORK_LOCATION/config.json --type common.Config --output $WORK_LOCATION/config.pb
configtxlator proto_encode --input $WORK_LOCATION/modified_config.json --type common.Config --output $WORK_LOCATION/modified_config.pb
configtxlator compute_update --channel_id $CHANNEL_NAME --original $WORK_LOCATION/config.pb --updated $WORK_LOCATION/modified_config.pb --output $WORK_LOCATION/org3_update.pb
configtxlator proto_decode --input $WORK_LOCATION/org3_update.pb --type common.ConfigUpdate | jq . > $WORK_LOCATION/org3_update.json
echo '{"payload":{"header":{"channel_header":{"channel_id":"'$CHANNEL_NAME'", "type":2}},"data":{"config_update":'$(cat $WORK_LOCATION/org3_update.json)'}}}' | jq . > $WORK_LOCATION/org3_update_in_envelope.json
configtxlator proto_encode --input $WORK_LOCATION/org3_update_in_envelope.json --type common.Envelope --output $WORK_LOCATION/org3_update_in_envelope.pb


