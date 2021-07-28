apk add curl
apk add  bind-tools

ORDERER_IP=$1
ORDERER_HOSTNAME=$2
ORDERER_PORT=$3
CHANNEL_NAME=$4
SHARED_FS_HOST=$5

echo $ORDERER_IP $ORDERER_HOSTNAME >> /etc/hosts
echo $(dig +short $HOSTNAME) peer0.${HOSTNAME} >> /etc/hosts

# Download orderer CA
SHARED_FS_USER=scray
SHARED_FS_PW=scray
curl --user $SHARED_FS_USER:$SHARED_FS_PW http://$SHARED_FS_HOST/ca/tlsca.example.com-cert.pem > /tmp/tlsca.example.com-cert.pem
export ORDERER_CA=/tmp/tlsca.example.com-cert.pem
export CHANNEL_NAME=$CHANNEL_NAME


export CORE_PEER_MSPCONFIGPATH=/mnt/conf/organizations/peerOrganizations/$HOSTNAME/users/Admin@$HOSTNAME/msp/
export CORE_PEER_ADDRESS=$CORE_PEER_ADDRESS

peer channel fetch 0 mychannel.block -o $ORDERER_HOSTNAME:$ORDERER_PORT -c $CHANNEL_NAME --tls --cafile $ORDERER_CA
peer channel join -b mychannel.block

echo "Query channel info"
echo "Wait 5s to join channel"

sleep 5
export CORE_PEER_MSPCONFIGPATH=/mnt/conf/organizations/peerOrganizations/$HOSTNAME/users/User1@$HOSTNAME/msp/
peer channel getinfo -c $CHANNEL_NAME