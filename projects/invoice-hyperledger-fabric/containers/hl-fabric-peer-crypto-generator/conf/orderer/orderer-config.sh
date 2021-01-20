#!/bin/bash



function createCryptos() {

	export PATH=~/git/fabric-samples/test-network/fabric-samples/bin:$PATH
    ./configure_crypto.sh $ORG_NAME $DOMAINE
    cryptogen generate --config=crypto.yaml --output=./organizations
    export FABRIC_CFG_PATH=$PWD
    
    ./configure_configtx.sh $ORG_NAME $DOMAINE
    configtxgen -configPath $PWD  -printOrg ${ORG_NAME}MSP > organizations/peerOrganizations/$DOMAINE/${ORG_NAME}.json
    zip -q -r $ORG_NAME.zip organizations/
    
    copyCertsToDefaultDir


  export PATH=~/git/fabric-samples/test-network/fabric-samples/bin:$PATH
  ./configure_crypto.sh $ORG_NAME $DOMAINE
  cryptogen generate --config=crypto.yaml --output=./organizations

  cryptogen generate --config=./crypto-config-org.yaml --output="organizations"
  cryptogen generate --config=./organizations/cryptogen/crypto-config-orderer.yaml --output="organizations"
  
  configtxgen -configPath ./configtx/ -profile EmptyOrdererGenesis -channelID system-channel -outputBlock ./system-genesis-block/genesis.block
}

# Generate orderer system channel genesis block.
function createConsortium() {
  which configtxgen
  if [ "$?" -ne 0 ]; then
    fatalln "configtxgen tool not found."
  fi

  infoln "Generating Orderer Genesis block"

  # Note: For some unknown reason (at least for now) the block file can't be
  # named orderer.genesis.block or the orderer will fail to launch!
  set -x
  configtxgen -profile TwoOrgsOrdererGenesis -channelID system-channel -outputBlock ./system-genesis-block/genesis.block
  res=$?
  { set +x; } 2>/dev/null
  if [ $res -ne 0 ]; then
    fatalln "Failed to generate orderer genesis block..."
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
