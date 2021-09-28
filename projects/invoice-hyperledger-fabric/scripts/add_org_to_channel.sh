# Manual tutorial https://hyperledger-fabric.readthedocs.io/en/release-2.2/channel_update_tutorial.html
# Download orderer ca



# export ORDERER_CA=/tmp/orderer_ca.pem
export CHANNEL_NAME=mychannel

apk add curl

# Get current configuration
curl --user 'scray:scray' http://10.14.128.38:30080/latest_configuration/mychannel/latest.json > latest.json
curl --user 'scray:scray' http://kubernetes.research.dev.seeburger.de:30080/add_requests/mychannel/OrgScrayMSP.json > org3.json

# Add org3 data to existing config
jq -s '.[0] * {"channel_group":{"groups":{"Application":{"groups": {"OrgScrayMSP":.[1]}}}}}' config.json ./org3.json > modified_config.json


configtxlator proto_encode --input config.json --type common.Config --output config.pb
configtxlator proto_encode --input modified_config.json --type common.Config --output modified_config.pb
configtxlator compute_update --channel_id $CHANNEL_NAME --original config.pb --updated modified_config.pb --output org3_update.pb
configtxlator proto_decode --input org3_update.pb --type common.ConfigUpdate | jq . > org3_update.json
echo '{"payload":{"header":{"channel_header":{"channel_id":"'$CHANNEL_NAME'", "type":2}},"data":{"config_update":'$(cat org3_update.json)'}}}' | jq . > org3_update_in_envelope.json
configtxlator proto_encode --input org3_update_in_envelope.json --type common.Envelope --output org3_update_in_envelope.pb

# Sign update by first admin
peer channel signconfigtx -f org3_update_in_envelope.pb

# Sign by second admin
export CORE_PEER_LOCALMSPID="Org2MSP"
export CORE_PEER_TLS_ROOTCERT_FILE=/opt/gopath/src/github.com/hyperledger/fabric/peer/organizations/peerOrganizations/org2.example.com/peers/peer0.org2.example.com/tls/ca.crt
export CORE_PEER_MSPCONFIGPATH=/opt/gopath/src/github.com/hyperledger/fabric/peer/organizations/peerOrganizations/org2.example.com/users/Admin@org2.example.com/msp
export CORE_PEER_ADDRESS=peer0.org2.example.com:9051
peer channel update -f org3_update_in_envelope.pb -c $CHANNEL_NAME -o orderer.example.com:7050 --tls --cafile $ORDERER_CA

