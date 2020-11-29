BASE_DIR=$PWD

function installHyperledgerTestNework() {
	if ! [ -x "$(command -v curl)" ]; then
	  echo 'Error: curl is not installed.' >&2
	  exit 1
	fi
	
	curl -sSL https://raw.githubusercontent.com/hyperledger/fabric/release-2.2/scripts/bootstrap.sh | bash -s
}


function getScrayChaincode() {
	if ! [ -x "$(command -v git)" ]; then
	  echo 'Error: git is not installed.' >&2
	  exit 1
	fi
	
	git clone --branch feature/contractDockUpdate  https://github.com/scray/scray.git
}

function setupExampleLedger() {
	cd fabric-samples/test-network
	./network.sh up createChannel -ca -s couchdb
	# ./network.sh up createChannel -ca -s couchdb
	./network.sh deployCC -ccn scray-invoice-example -ccl java -ccp $BASE_DIR/scray/projects/invoice-hyperledger-fabric/chaincode/invoice/java
}

function shutNetworkDown() {
	./network.sh down
	
	cd fabric-samples/test-network
	./network.sh down
	
	cd ../../ && rm -fr fabric-samples
}

function installNetworkWithChaincode() {

	installHyperledgerTestNework
	getScrayChaincode
	setupExampleLedger
}


while [[ $# -ge 0 ]] ; do
  key="$1"
  case $key in
  -purge )
    shutNetworkDown
    exit 0
    ;;
  * )
	installNetworkWithChaincode
    exit 0
    ;;
  esac
  shift
done