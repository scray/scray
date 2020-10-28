BASE_DIR=$PWD

curl -sSL https://raw.githubusercontent.com/hyperledger/fabric/release-2.2/scripts/bootstrap.sh | bash -s
git clone --branch feature/contractDockUpdate  https://github.com/scray/scray.git

cd fabric-samples/test-network
./network.sh up createChannel
./network.sh deployCC -ccn scray-invoice-example -ccl java -ccp $BASE_DIR/scray/projects/invoice-hyperledger-fabric/chaincode/invoice/java