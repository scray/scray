#!/bin/bash

GEN_BLOCK_PATH=./system-genesis-block
CREATE_ADMIN_ORG=true

copyCertsToDefaultDir() {
    cp -r organizations/ordererOrganizations/${DOMAINE}/orderers/$ORG_NAME.${DOMAINE}/msp ./
    cp -r organizations/ordererOrganizations/${DOMAINE}/orderers/$ORG_NAME.${DOMAINE}/tls ./
}

function createCryptos() {
  echo "Create crypto material"
  
   # Create admin org
   if [ "$CREATE_ADMIN_ORG"=true ];
   then
     createAdminOrg
    fi

	echo "Create "
    export PATH=~/git/fabric-samples/bin:$PATH
    ./configure_crypto.sh -o $ORG_NAME -d $DOMAINE
    cryptogen generate --config=./target/crypto-config-orderer.yaml --output="organizations"

    
    res=$?
    { set +x; } 2>/dev/null
    if [ $res -ne 0 ]; then
       "Failed to generate certificates..." 1>&2
    fi
    
    export FABRIC_CFG_PATH=$PWD
    copyCertsToDefaultDir
}

function createAdminOrg() {
	cd ../admin/
	./create-admin-org.sh
	cd ../orderer/
}

# Generate orderer system channel genesis block.
function createConsortium() {
  export PATH=~/git/fabric-samples/bin:$PATH
  which configtxgen
  if [ "$?" -ne 0 ]; then
    fatalln "configtxgen tool not found."
  fi

#  ./configure_configtx.sh $ORG_NAME $DOMAINE

  echo "Generating Orderer Genesis block"

  set -x
  configtxgen -profile TwoOrgsOrdererGenesis -channelID system-channel -outputBlock $GEN_BLOCK_PATH/genesis.block 
  res=$?
  { set +x; } 2>/dev/null
  if [ $res -ne 0 ]; then
    echo "Failed to generate orderer genesis block..."
  fi
}

usage()
{
    echo "usage: Configure orderer node [[[-o ] [-d]] | [-h]]"
}


while [ "$1" != "" ]; do
    case $1 in
        -o | --organization )   shift
                                ORG_NAME=$1
                                ;;
        -d | --domain )   	shift
	       			DOMAINE=$1	
                                ;;
        -g | --gennesis.block-path )         shift
                                GEN_BLOCK_PATH=$1
				;;
        -a | --create-admin-org )	shift
                                CREATE_ADMIN_ORG=$1
				;;
        -h | --help )           usage
                                exit
                                ;;
        * )                     # usage
                                exit 1
    esac
    shift
done

echo "Configuration"
echo "  ORG_NAME: ${ORG_NAME}"
echo "  DOMAINE: ${DOMAINE}"

createCryptos
createConsortium
