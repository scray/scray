#!/bin/bash

GEN_BLOCK_PATH=./system-genesis-block


copyCertsToDefaultDir() {
    cp -r organizations/ordererOrganizations/${DOMAINE}/orderers/orderer.${DOMAINE}/msp ./
    cp -r organizations/ordererOrganizations/${DOMAINE}/orderers/orderer.${DOMAINE}/tls ./
}

function createCryptos() {
  echo "Create crypto material"

    export PATH=~/git/fabric-samples/test-network/fabric-samples/bin:$PATH
    ./configure_crypto.sh -o $ORG_NAME -d $DOMAINE
    cryptogen generate --config=./target/crypto-config-orderer.yaml --output="organizations"
    res=$?
    { set +x; } 2>/dev/null
    if [ $res -ne 0 ]; then
      fatalln "Failed to generate certificates..."
    fi
    
    export FABRIC_CFG_PATH=$PWD
    
    copyCertsToDefaultDir
}

# Generate orderer system channel genesis block.
function createConsortium() {
  which configtxgen
  if [ "$?" -ne 0 ]; then
    fatalln "configtxgen tool not found."
  fi

#  ./configure_configtx.sh $ORG_NAME $DOMAINE

  echo "Generating Orderer Genesis block"

  # Note: For some unknown reason (at least for now) the block file can't be
  # named orderer.genesis.block or the orderer will fail to launch!
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
        -g | --gennesis.block-path )         shift
                                GEN_BLOCK_PATH=$1
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

createCryptos
createConsortium
