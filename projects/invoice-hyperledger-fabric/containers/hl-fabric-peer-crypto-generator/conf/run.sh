#!/bin/bash

DOMAINE=org1.fabric.hyperledger.projects.scray.org
ORG_NAME=OrgScrayMSP
CHANNEL_NAME=mychannel
SHARED_FS_HOST=10.15.136.41:30080
SHARED_FS_USER=scray
SHARED_FS_PW=scray
BASE_PATH=$PWD


copyCertsToDefaultDir() {
    cp -r organizations/peerOrganizations/${DOMAINE}/peers/peer0.${DOMAINE}/msp ./
    cp -r organizations/peerOrganizations/${DOMAINE}/peers/peer0.${DOMAINE}/tls ./
}

yq() {
  $BASE_PATH/bin/yq $1 $2 $3 $4 $5
}

# Check if yq exists
checkYqVersion() {
  dowloadYqBin
}

dowloadYqBin() {
  if [[ ! -f "./bin/yq" ]]
  then
    echo "yq does not exists"
    echo "download linux_amd64 yq binary"
    
    mkdir bin
    curl -L https://github.com/mikefarah/yq/releases/download/3.4.1/yq_linux_amd64 -o ./bin/yq
    chmod u+x ./bin/yq
  fi
}

createPeerConfig() {
    
    export PATH=~/git/fabric-samples/test-network/fabric-samples/bin:$PATH
    ./configure_crypto.sh $ORG_NAME $DOMAINE
    cryptogen generate --config=crypto.yaml --output=./organizations
    export FABRIC_CFG_PATH=$PWD
    
    ./configure_configtx.sh $ORG_NAME $DOMAINE
    configtxgen -configPath $PWD  -printOrg ${ORG_NAME}MSP > organizations/peerOrganizations/$DOMAINE/${ORG_NAME}.json
    zip -q -r $ORG_NAME.zip organizations/
    
    copyCertsToDefaultDir
    
    curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/newmemberrequests/
    curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/newmemberrequests/$CHANNEL_NAME
    curl --user $SHARED_FS_USER:$SHARED_FS_PW -X DELETE http://$SHARED_FS_HOST/newmemberrequests/$CHANNEL_NAME/${ORG_NAME}.json
    curl --user $SHARED_FS_USER:$SHARED_FS_PW -T organizations/peerOrganizations/$DOMAINE/${ORG_NAME}.json http://$SHARED_FS_HOST/newmemberrequests/$CHANNEL_NAME/${ORG_NAME}.json
    
    # Upload CA
    curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/ca
    curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/ca/$CHANNEL_NAME
    curl --user $SHARED_FS_USER:$SHARED_FS_PW -T organizations/peerOrganizations/$DOMAINE/users/User1@$DOMAINE/tls/ca.crt http://$SHARED_FS_HOST/ca/$CHANNEL_NAME/$ORG_NAME-$DOMAINE-ca.crt
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
        -t | --node-type )         shift
                                NODE_TYPE=$1
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
echo "  NODE_TYPE: ${NODE_TYPE}"
echo "  ORG_NAME: ${ORG_NAME}"
echo "  DOMAINE: ${DOMAINE}"

checkYqVersion

if [ "$NODE_TYPE" = orderer ]
then
	echo "Configure node as orderer"
	cd orderer
	./configure_orderer.sh -o $ORG_NAME -d $DOMAINE
elif [ "$NODE_TYPE" = peer ]
then
  echo "Configure node as peer"
  createPeerConfig
else
  echo "Unknown node type. Treat it as a peer"
fi
