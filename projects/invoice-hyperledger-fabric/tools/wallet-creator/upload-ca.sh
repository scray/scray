#!/bin/bash

SHARED_FS_HOST=hl-fabric-data-share-service:80
SHARED_FS_USER=scray
SHARED_FS_PW=scray

ORG_NAME=$1
DOMAINE=$2

curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL  http://$SHARED_FS_HOST/peers
curl --user $SHARED_FS_USER:$SHARED_FS_PW -X MKCOL  http://$SHARED_FS_HOST/peers/$ORG_NAME
curl --user $SHARED_FS_USER:$SHARED_FS_PW -X DELETE http://$SHARED_FS_HOST/peers/$ORG_NAME/ca.pem
curl --user $SHARED_FS_USER:$SHARED_FS_PW -X DELETE http://$SHARED_FS_HOST/peers/$ORG_NAME/ca.key
curl --user $SHARED_FS_USER:$SHARED_FS_PW -T /mnt/conf/organizations/peerOrganizations/$DOMAINE/ca/ca.${DOMAINE}-cert.pem http://$SHARED_FS_HOST/peers/$ORG_NAME/ca.pem
curl --user $SHARED_FS_USER:$SHARED_FS_PW -T /mnt/conf/organizations/peerOrganizations/$DOMAINE/ca/priv_sk http://$SHARED_FS_HOST/peers/$ORG_NAME/ca.key
