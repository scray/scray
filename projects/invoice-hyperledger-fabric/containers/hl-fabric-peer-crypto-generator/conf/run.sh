#!/bin/bash

DOMAINE=org1.fabric.hyperledger.projects.scray.org
ORG_NAME=OrgScrayMSP
CHANNEL_NAME=mychannel2
SHARED_FS_HOST=10.15.136.41:30080
SHARED_FS_USER=scray
SHARED_FS_PW=scray

createConfig() {
# Add repo for yq
echo "@community http://dl-cdn.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories
apk add yq@community


export PATH=~/git/fabric-samples/test-network/fabric-samples/bin:$PATH
./configure_crypto.sh $ORG_NAME $DOMAINE
cryptogen generate --config=crypto.yaml --output=./organizations
export FABRIC_CFG_PATH=$PWD

./configure_configtx.sh $ORG_NAME $DOMAINE
configtxgen -configPath $PWD  -printOrg ${ORG_NAME}MSP > organizations/peerOrganizations/$DOMAINE/${ORG_NAME}.json
zip -q -r $ORG_NAME.zip organizations/


curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/add_requests
curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/add_requests/$CHANNEL_NAME
curl --user $SHARED_FS_USER:$SHARED_FS_PW -X DELETE http://$SHARED_FS_HOST/add_requests/$CHANNEL_NAME/${ORG_NAME}.json
curl --user $SHARED_FS_USER:$SHARED_FS_PW -T organizations/peerOrganizations/$DOMAINE/${ORG_NAME}.json http://$SHARED_FS_HOST/add_requests/$CHANNEL_NAME/${ORG_NAME}.json

# Upload CA
curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/ca
curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/ca/$CHANNEL_NAME
curl --user $SHARED_FS_USER:$SHARED_FS_PW -T organizations/peerOrganizations/$DOMAINE/users/User1@$DOMAINE/tls/ca.crt http://$SHARED_FS_HOST/ca/$CHANNEL_NAME/$DOMAINE-ca.crt
}

usage()
{
    echo "usage: Prepare peer node [[[-o ] [-d]] | [-h]]"
}


while [ "$1" != "" ]; do
    case $1 in
        -o | --organization )   shift
                                ORG_NAME=$1
                                ;;
        -d | --domain )   	shift
	       			DOMAINE=$1	
                                ;;
        -h | --help )           usage
                                exit
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done

echo "Configuration"
echo "  ORG_NAME: ${ORG_NAME}"
echo "  DOMAINE: ${DOMAINE}"

createConfig
