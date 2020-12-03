apk add curl

ORDERER_IP=$1
ORDERER_HOSTNAME=$2
PEER_IP=$2

echo $ORDERER_IP $ORDERER_HOSTNAME >> /etc/hosts
#echo $PEER_IP peer0.org1.fabric.hyperledger.projects.scray.org >> /etc/hosts

# Download orderer CA
SHARED_FS_HOST=10.15.136.41:30080
SHARED_FS_USER=scray
SHARED_FS_PW=scray
curl --user $SHARED_FS_USER:$SHARED_FS_PW http://$SHARED_FS_HOST/ca/tlsca.example.com-cert.pem > /tmp/tlsca.example.com-cert.pem
export ORDERER_CA=/tmp/tlsca.example.com-cert.pem
export CHANNEL_NAME=mychannel
export CORE_PEER_ADDRESS=peer0.kubernetes.research.dev.seeburger.de:30003

peer channel fetch 0 mychannel.block -o $ORDERER_HOSTNAME:7050 -c $CHANNEL_NAME --tls --cafile $ORDERER_CA
peer channel join -b mychannel.block
