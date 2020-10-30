# Manual tutorial https://hyperledger-fabric.readthedocs.io/en/release-2.2/channel_update_tutorial.html
# Download orderer ca

wget https://mft.seeburger.de:443/portal-seefx/~public/Mjg1ZTA0ODctY2E1MC00Yzg1LWEzYmEtODg3OTA0MDZkYTE1?download -O /tmp/orderer_ca.pem


export ORDERER_CA=/tmp/orderer_ca.pem
export CHANNEL_NAME=mychannel

apk install curl

# Get current configuration
curl --user 'scray:scray' http://10.14.128.38:30080/latest_configuration/mychannel/latest.json > latest.json
curl --user 'scray:scray' http://10.14.128.38:30080/add_requests/mychannel/org3.json > org3.json

# Add org3 data to existing config
jq -s '.[0] * {"channel_group":{"groups":{"Application":{"groups": {"Org3MSP":.[1]}}}}}' config.json ./org3.json > modified_config.json


configtxlator proto_encode --input config.json --type common.Config --output config.pb
configtxlator proto_encode --input modified_config.json --type common.Config --output modified_config.pb
configtxlator compute_update --channel_id $CHANNEL_NAME --original config.pb --updated modified_config.pb --output org3_update.pb
configtxlator proto_decode --input org3_update.pb --type common.ConfigUpdate | jq . > org3_update.json
echo '{"payload":{"header":{"channel_header":{"channel_id":"'$CHANNEL_NAME'", "type":2}},"data":{"config_update":'$(cat org3_update.json)'}}}' | jq . > org3_update_in_envelope.json
configtxlator proto_encode --input org3_update_in_envelope.json --type common.Envelope --output org3_update_in_envelope.pb

# Sign update by first admin
peer channel signconfigtx -f org3_update_in_envelope.pb

peer channel update -f org3_update_in_envelope.pb -c $CHANNEL_NAME -o orderer.example.com:7050 --tls --cafile $ORDERER_CA

