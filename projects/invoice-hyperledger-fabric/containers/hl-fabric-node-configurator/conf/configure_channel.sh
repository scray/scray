#!/bin/bash
NEW_ORG=$1
DOMAINE=$2
CONFIGTX_FILE=./configtx.yaml.channel.org
CONFIGTX_TMP="configtx.yaml"
DEFAULT_ORG=OrgScray
PORT=30001

ORG_MSP_NAME="${NEW_ORG}MSP"

BASE_PATH=$PWD

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

checkYqVersion

cp $CONFIGTX_FILE configtx.yaml

# Update anchor name 
sed "s/$DEFAULT_ORG/$NEW_ORG/g" $CONFIGTX_FILE > $CONFIGTX_TMP 

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
yq w -i $CONFIGTX_TMP --style=double "Organizations[0].Policies.Readers.Rule" $defaultReaderRule 

defaultWriterRule="OR('${ORG_MSP_NAME}.admin', '${ORG_MSP_NAME}.client')"
yq w -i $CONFIGTX_TMP --style=double  "Organizations[0].Policies.Writers.Rule" ${defaultWriterRule}

defaultAdminRule="OR('${ORG_MSP_NAME}.admin')"
yq w -i $CONFIGTX_TMP --style=double  "Organizations[0].Policies.Admins.Rule" ${defaultAdminRule}

defaultEndorsementRule="OR('${ORG_MSP_NAME}.peer')"
yq w -i $CONFIGTX_TMP --style=double  "Organizations[0].Policies.Endorsement.Rule" ${defaultEndorsementRule}

yq w -i $CONFIGTX_TMP "Organizations[0].AnchorPeers[0].Host" $DOMAINE

yq w -i $CONFIGTX_TMP "Organizations[0].AnchorPeers[0].Port" $PORT

cat $CONFIGTX_TMP 
