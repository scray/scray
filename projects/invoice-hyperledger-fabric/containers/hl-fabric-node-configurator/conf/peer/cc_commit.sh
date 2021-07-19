CHANNEL_ID=$1

export PKGID=basic_1.0:5a294a12a1a89cd4eed3d4234fbc79f42eab2ac20cd176bc8ebbc07c597cd0ee

export CORE_PEER_MSPCONFIGPATH=/mnt/conf/organizations/peerOrganizations/$HOSTNAME/users/Admin@$HOSTNAME/msp/

echo Installed chaincode
peer lifecycle chaincode queryinstalled

peer lifecycle chaincode checkcommitreadiness -o orderer.example.com:7050 --ordererTLSHostnameOverride orderer.example.com --tls  --cafile /tmp/tlsca.example.com-cert.pem --channelID $CHANNEL_ID --name basic --sequence 1 --version 1.0

peer lifecycle chaincode commit -o orderer.example.com:7050 --tls  --cafile /tmp/tlsca.example.com-cert.pem --channelID $CHANNEL_ID --name basic --version 1.0 --sequence 1
