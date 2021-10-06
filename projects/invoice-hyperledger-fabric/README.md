# Hyperledger Fabric projects

This project uses the [Hyperledger Fabric](https://www.hyperledger.org/use/fabric) to realize some POCs with this technology.  
It 'kubernetze' Hyperledger Fabric components like the 
* [Ordering Service](containers/orderer/README.md), 
* [Peer Nodes](containers/README.md), 
* [External Smart Contracts](chaincode/chaincode-external/README.md)  

and provides tooling to interact with the blockchain network.  
With features like:
* [Creating a channel](containers/orderer/README.md#create-new-channel) 
* [Adding peers to the network](containers/README.md#integrate-new-peer-to-scray-k8s-network)
* [Create a wallet from local X509 CSR](tools/wallet-creator#create-user-wallet-for-a-given-ca)
* [Export monitoring data](containers/README.md#export-data)
* [Reading and write using the Hyper Ledger Fabric Gateway](applications/asset-reader-writer-app#application-to-interact-with-blockchain)
* [Example application which is creating and transferring invoices](chaincode/chaincode-external#write-own-invoices)


## Components
* [Orderer](containers/orderer/README.md) Component to order transactions. For details see  [Hyperleder Fabric doc](https://hyperledger-fabric.readthedocs.io/en/release-2.3/orderer/ordering_service.html#)
* [Peer](containers/README.md) Peer component. For details see [Hyperleder Fabric doc](https://hyperledger-fabric.readthedocs.io/en/release-2.3/orderer/ordering_service.html#)
* [External Chaincode](chaincode/chaincode-external/README.md) Implementation of an chaincode which runns as a service
* [Client App](applications/asset-reader-writer-app) Client application to read and write to the blockchain
