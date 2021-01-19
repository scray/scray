package org.scray.projects.hyperledger_fabric.invoice_service;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.hyperledger.fabric.gateway.Contract;
import org.hyperledger.fabric.gateway.Gateway;
import org.hyperledger.fabric.gateway.Network;
import org.hyperledger.fabric.gateway.Wallet;
import org.hyperledger.fabric.gateway.Wallets;


public class App {

	static {
		System.setProperty("org.hyperledger.fabric.sdk.service_discovery.as_localhost", "false");
	}

	// helper function for getting connected to the gateway
	public static Gateway connect(BasicConfigParameters parmas) throws Exception{
		// Load a file system based wallet for managing identities.
		Path walletPath = Paths.get("wallet");
		Wallet wallet = Wallets.newFileSystemWallet(walletPath);
		// load a CCP
		Path networkConfigPath = Paths.get(parmas.getNetworkConfigPath());

		for (String path : wallet.list()) {
			System.out.println(path);
		}
		
		Gateway.Builder builder = Gateway.createBuilder();
		builder.identity(wallet, "admin").networkConfig(networkConfigPath).discovery(true);
		return builder.connect();
	}

	public static void main(String[] args) throws Exception {
	    BasicConfigParameters params = new BasicConfigParameters();
	    params.setNetworkConfigPath("/home/stefan/libs/fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/connection-org1.yaml");
	    params.setNetworkConfigPath("/home/stefan/libs/fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/connection-org1.yaml");
	    params.setCaCertPem("/home/stefan/libs/fabric-samples/test-network/organizations/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp/cacerts/localhost-7054-ca-org1.pem");
	    params.setHyperlederHost("localhost");
	    
	    
//	    for (int i = 0; i < args.length; i++) {
//            if(args[i].startsWith("--networkConfigPath")) {
//                if((i + 1) < (args.length -1)) {
//                    i += 1;
//                    params.setNetworkConfigPath(args[i]);
//                }
//            }
//            
//            if(args[i].startsWith("--caCertPem")) {
//                if((i + 1) < (args.length -1)) {
//                    i += 1;
//                    params.setCaCertPem(args[i]);
//                }
//            }
//            
//            if(args[i].startsWith("--hyperlederHost")) {
//                if((i + 1) < (args.length -1)) {
//                    i += 1;
//                    params.setHyperlederHost(args[i]);
//                }
//            }
//        }
	    
	    System.out.println(params);
	    
	    
		// enrolls the admin and registers the user
		try {
			EnrollAdmin.main(null);
			//RegisterUser.main(null, params);
		} catch (Exception e) {
			System.err.println(e);
		}

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

					System.out.println("\n");
					result = contract.evaluateTransaction("GetAllAssets");
					System.out.println("Evaluate Transaction: GetAllAssets, result: " + new String(result));

					System.out.println("\n");
					System.out.println("Submit Transaction: CreateAsset asset13");
					//CreateAsset creates an asset with ID asset13, color yellow, owner Tom, size 5 and appraisedValue of 1300
					contract.submitTransaction("CreateAsset", "asset13", "yellow", "5", "Tom", "1300");

					System.out.println("\n");
					System.out.println("Evaluate Transaction: ReadAsset asset13");
					// ReadAsset returns an asset with given assetID
					result = contract.evaluateTransaction("ReadAsset", "asset13");
					System.out.println("result: " + new String(result));

					System.out.println("\n");
					System.out.println("Evaluate Transaction: AssetExists asset1");
					// AssetExists returns "true" if an asset with given assetID exist
					result = contract.evaluateTransaction("AssetExists", "asset1");
					System.out.println("result: " + new String(result));

					System.out.println("\n");
					System.out.println("Submit Transaction: UpdateAsset asset1, new AppraisedValue : 350");
					// UpdateAsset updates an existing asset with new properties. Same args as CreateAsset
					contract.submitTransaction("UpdateAsset", "asset1", "blue", "5", "Tomoko", "350");

					System.out.println("\n");
					System.out.println("Evaluate Transaction: ReadAsset asset1");
					result = contract.evaluateTransaction("ReadAsset", "asset1");
					System.out.println("result: " + new String(result));

					try {
						System.out.println("\n");
						System.out.println("Submit Transaction: UpdateAsset asset70");
						//Non existing asset asset70 should throw Error
						contract.submitTransaction("UpdateAsset", "asset70", "blue", "5", "Tomoko", "300");
					} catch (Exception e) {
						System.err.println("Expected an error on UpdateAsset of non-existing Asset: " + e);
					}

					System.out.println("\n");
					System.out.println("Submit Transaction: TransferAsset asset1 from owner Tomoko > owner Tom");
					// TransferAsset transfers an asset with given ID to new owner Tom
					contract.submitTransaction("TransferAsset", "asset1", "Tom");

					System.out.println("\n");
					System.out.println("Evaluate Transaction: ReadAsset asset1");
					result = contract.evaluateTransaction("ReadAsset", "asset1");
					System.out.println("result: " + new String(result));
				}
				catch(Exception e){
					e.printStackTrace();
					System.err.println(e);
				}
	}
}
