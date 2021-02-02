#!/bin/bash
BASE_PATH=$PWD
ORG_NAME=$1
DOMAINE=$2
ORG_CRYPTO_CONFIG_FILE=crypto-config-orderer.yaml
YQ_VERSON=4.4.1

DEBUG=false



yq() {
  $BASE_PATH/bin/yq-${YQ_VERSON} $1 $2 $3 $4 $5
}

# Check if yq exists
checkYqVersion() {
  dowloadYqBin
}

dowloadYqBin() {
  if [[ ! -f "./bin/yq-${YQ_VERSON}" ]]
  then
    echo "yq does not exists"
    echo "download linux_amd64 yq binary"
    
    mkdir bin
    curl -L https://github.com/mikefarah/yq/releases/download/v${YQ_VERSON}/yq_linux_amd64 -o ./bin/yq-${YQ_VERSON}
    chmod u+x ./bin/yq-${YQ_VERSON}
  fi
}

clean() {
	rm -fr ./bin
	rm -fr ./target
}



customozeConfigFile() {
	TARGET_PATH=./target/$ORDERER_NAME
	
	mkdir -p $TARGET_PATH
	cp $ORG_CRYPTO_CONFIG_FILE $TARGET_PATH
	echo $ORG_CRYPTO_CONFIG_FILE
	cd $TARGET_PATH

	# Update name
	yq -i eval ".OrdererOrgs[0].Name=\"$ORG_NAME\"" $ORG_CRYPTO_CONFIG_FILE

	# Update Domain 
	yq -i eval ".OrdererOrgs[0].Domain=\"$DOMAINE\"" $ORG_CRYPTO_CONFIG_FILE
	
	yq -i eval ".OrdererOrgs[0].Specs[0].Hostname=\"$ORG_NAME\"" $ORG_CRYPTO_CONFIG_FILE
	
	# Add SANS
	YQ_CHANGE_COMMAND=$(echo \''.OrdererOrgs[0].Specs[0].SANS += '\"$DOMAINE\"\') 
	echo "yq  -i eval $YQ_CHANGE_COMMAND crypto-config-orderer.yaml " > update_SANS.sh
	chmod u+x update_SANS.sh
	./update_SANS.sh

	cat $ORG_CRYPTO_CONFIG_FILE
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
	    -c | --clean )   shift
                   clean
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

dowloadYqBin
customozeConfigFile
