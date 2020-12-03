EXT_CC_IP=$1
HOST_IP=$2
CHANNEL_ID=mychannel

export PKGID=basic_1.0:5a294a12a1a89cd4eed3d4234fbc79f42eab2ac20cd176bc8ebbc07c597cd0ee

# Set hostname of external chaincode node
echo $EXT_CC_IP asset-transfer-basic.org1.example.com >> /etc/hosts
echo $HOST_IP peer0.org1.example.com peer0.org2.example.com orderer.example.com >> /etc/hosts

# Get chaincode description

curl https://mft.seeburger.de:443/portal-seefx/~public/MDI0Mjk4ZTQtZGQ3ZS00M2Y4LWIyMDktZjY1YzljN2MwMTlm?download > chaincode_description.tgz
peer lifecycle chaincode install chaincode_description.tgz

peer lifecycle chaincode approveformyorg -o orderer.example.com:7050 --ordererTLSHostnameOverride orderer.example.com --tls  --cafile /tmp/tlsca.example.com-cert.pem --channelID $CHANNEL_ID --name basic --version 1.0 --package-id $PKGID --sequence 1