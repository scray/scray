DOMAINE=org1.fabric.hyperledger.projects.scray.org
ORG_NAME=OrgScrayMSP
CHANNEL_NAME=mychannel
SHARED_FS_HOST=10.15.136.41:30080
SHARED_FS_USER=scray
SHARED_FS_PW=scray


export PATH=~/git/fabric-samples/test-network/fabric-samples/bin:$PATH
cryptogen generate --config=org-scray-crypto.yaml --output=./organizations
export FABRIC_CFG_PATH=$PWD

configtxgen -configPath $PWD  -printOrg $ORG_NAME > organizations/peerOrganizations/$DOMAINE/${ORG_NAME}.json
zip -q -r $ORG_NAME.zip organizations/

curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/add_requests
curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL http://$SHARED_FS_HOST/add_requests/$CHANNEL_NAME
curl --user $SHARED_FS_USER:$SHARED_FS_PW -X DELETE http://$SHARED_FS_HOST/add_requests/$CHANNEL_NAME/${ORG_NAME}.json
curl --user $SHARED_FS_USER:$SHARED_FS_PW -T organizations/peerOrganizations/$DOMAINE/${ORG_NAME}.json http://$SHARED_FS_HOST/add_requests/$CHANNEL_NAME/${ORG_NAME}.json

