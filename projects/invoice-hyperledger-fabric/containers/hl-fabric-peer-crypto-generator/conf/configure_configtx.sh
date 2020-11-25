#!/bin/bash
CONFIGTX_FILE=./configtx.yaml
CONFIGTX_TMP=configtx.yaml.tmp
DEFAULT_ORG=OrgScray
NEW_ORG=$1
DOMAINE=$2

ORG_MSP_NAME="${NEW_ORG}MSP"

# Update anchor name 
sed "s/&$DEFAULT_ORG/ \&$NEW_ORG/g" $CONFIGTX_FILE > $CONFIGTX_TMP 

# Update name
yq w -i $CONFIGTX_TMP  "Organizations[0].Name" $ORG_MSP_NAME
# Update id
yq w -i $CONFIGTX_TMP  "Organizations[0].ID" $ORG_MSP_NAME

# Update msp dir
newMSPDir="organizations/peerOrganizations/${DOMAINE}/msp"
yq w -i $CONFIGTX_TMP  "Organizations[0].MSPDir" $newMSPDir

# Set default Reader rules
defaultReaderRule="OR('${ORG_MSP_NAME}.admin', '${ORG_MSP_NAME}.peer', '${ORG_MSP_NAME}.client')"
echo $defaultReaderRule
yq w -i $CONFIGTX_TMP --style=double  "Organizations[0].Policies.Readers.Rule" "${defaultReaderRule}" 



cat $CONFIGTX_TMP 
