CHANNEL_ID=$1
ASSET_ID=$2
NEW_OWNER=$3

export PKGID=basic_1.0:5a294a12a1a89cd4eed3d4234fbc79f42eab2ac20cd176bc8ebbc07c597cd0ee

export CORE_PEER_MSPCONFIGPATH=/mnt/conf/organizations/peerOrganizations/$HOSTNAME/users/User1@$HOSTNAME/msp/

peer chaincode invoke -o orderer.example.com:7050 --tls --cafile /tmp/tlsca.example.com-cert.pem -C $CHANNEL_ID -n basic -c  '{"function":"TransferAsset","Args":["'${ASSET_ID}'", "'${NEW_OWNER}'"]}'
peer chaincode query -C $CHANNEL_ID -n basic -c '{"Args":["GetAllAssets"]}'