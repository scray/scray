#!/bin/bash
NEW_ORG=$1
DOMAIN=$2
ORG_CRYPTO_CONFIG_FILE=crypto.yaml.org
CRYPTO_CONFIG_FILE="configtx.yaml"


cp $ORG_CRYPTO_CONFIG_FILE $CRYPTO_CONFIG_FILE 
echo $CRYPTO_CONFIG_FILE
echo $ORG_CRYPTO_CONFIG_FILE

# Update name
yq w -i $CRYPTO_CONFIG_FILE  "PeerOrgs[0].Name" $NEW_ORG 
# Update Domain 
yq w -i $CRYPTO_CONFIG_FILE  "PeerOrgs[0].Domain" $DOMAIN

