package org.scray.projects.hyperledger_fabric.invoice_service;

import org.hyperledger.fabric.gateway.*;

import java.nio.file.Path;
import java.nio.file.Paths;


public class GetAllAssetsApp {
	final static String FABRIC_SAMPLES_BASE_PATH = System.getProperty("user.home") + "/" ;

	static {
		System.setProperty("org.hyperledger.fabric.sdk.service_discovery.as_localhost", "false");
	}

	// helper function for getting connected to the gateway
	public static Gateway connect(BasicConfigParameters parmas) throws Exception{
		// Load a file system based wallet for managing identities.
		Path walletPath = Paths.get(System.getProperty("user.home") + "/git/scray/projects/invoice-hyperledger-fabric/tools/wallet-creator/wallet");
		Wallet wallet = Wallets.newFileSystemWallet(walletPath);
		// load a CCP
		Path networkConfigPath = Paths.get(parmas.getNetworkConfigPath());

		for (String path : wallet.list()) {
			System.out.println(path);
		}
		
		Gateway.Builder builder = Gateway.createBuilder();
		builder.identity(wallet, "Alice").networkConfig(networkConfigPath).discovery(true);
		return builder.connect();
	}

	public static void main(String[] args) throws Exception {
	    BasicConfigParameters params = new BasicConfigParameters();
	    params.setNetworkConfigPath(FABRIC_SAMPLES_BASE_PATH + "fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/connection-org1.yaml");
	    //params.setNetworkConfigPath("/home/stefan/libs/fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/connection-org1.yaml");
	    params.setCaCertPem(FABRIC_SAMPLES_BASE_PATH + "fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/ca/tlsca.org1.example.com-cert.pem");
	    params.setHyperlederHost("kubernetes.research.dev.seeburger.de");

	    System.out.println(params);

		interactWithBC(params);
	}
	
	public static void interactWithBC(BasicConfigParameters params) {
		
				// connect to the network and invoke the smart contract
				try (Gateway gateway = connect(params)) {

					// get the network and contract
					Network network = gateway.getNetwork("mychannel");

					Contract contract = network.getContract("basic");

					byte[] result;

					System.out.println("Submit Transaction: InitLedger creates the initial set of assets on the ledger.");
					contract.submitTransaction("InitLedger");

					//System.out.println("\n");
					//result = contract.evaluateTransaction("GetAllAssets");
					//System.out.println("Evaluate Transaction: GetAllAssets, result: " + new String(result));
				}
				catch(Exception e){
					e.printStackTrace();
					System.err.println(e);
				}
	}
}
