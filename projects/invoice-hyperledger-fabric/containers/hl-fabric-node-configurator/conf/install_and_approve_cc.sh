EXT_CC_IP=$1
ORDERER_IP=$2
ORDERER_HOSTNAME=$3
ORDERER_PORT=$4
CHANNEL_ID=$5

export PKGID=basic_1.0:5a294a12a1a89cd4eed3d4234fbc79f42eab2ac20cd176bc8ebbc07c597cd0ee

echo $ORDERER_IP $ORDERER_HOSTNAME >> /etc/hosts
# Set hostname of external chaincode node
echo $EXT_CC_IP asset-transfer-basic.org1.example.com >> /etc/hosts


export CHANNEL_NAME=$CHANNEL_NAME

export CORE_PEER_MSPCONFIGPATH=/mnt/conf/organizations/peerOrganizations/$HOSTNAME/users/Admin@$HOSTNAME/msp/
export CORE_PEER_ADDRESS=$CORE_PEER_ADDRESS

# Get chaincode description
curl https://mft.seeburger.de:443/portal-seefx/~public/MDI0Mjk4ZTQtZGQ3ZS00M2Y4LWIyMDktZjY1YzljN2MwMTlm?download > chaincode_description.tgz
peer lifecycle chaincode install chaincode_description.tgz

peer lifecycle chaincode queryinstalled
peer lifecycle chaincode approveformyorg      -o $ORDERER_HOSTNAME:$ORDERER_PORT --tls  --cafile /tmp/tlsca.example.com-cert.pem --channelID $CHANNEL_ID --name basic --version 1.0 --package-id $PKGID --sequence 1
peer lifecycle chaincode checkcommitreadiness -o $ORDERER_HOSTNAME:$ORDERER_PORT --ordererTLSHostnameOverride orderer.example.com --tls  --cafile /tmp/tlsca.example.com-cert.pem --channelID $CHANNEL_ID --name basic --version 1.0 --sequence 1
