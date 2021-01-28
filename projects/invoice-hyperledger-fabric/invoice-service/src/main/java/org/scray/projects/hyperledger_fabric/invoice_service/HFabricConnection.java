package org.scray.projects.hyperledger_fabric.invoice_service;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.hyperledger.fabric.gateway.Contract;
import org.hyperledger.fabric.gateway.Gateway;
import org.hyperledger.fabric.gateway.Network;
import org.hyperledger.fabric.gateway.Wallet;
import org.hyperledger.fabric.gateway.Wallets;

public class HFabricConnection {

	private Contract contract = null;
	private BasicConfigParameters parmas =  null;
	
	

	public HFabricConnection(BasicConfigParameters parmas) {
        super();
        this.parmas = parmas;
    }


    public void createConnectionInvoceContract() throws Exception {
		Gateway gateway = connect();

			// get the network and contract
			Network network = gateway.getNetwork("mychannel");
			Contract contract = network.getContract("basic");

			this.contract = contract;
			
			

	}
	
	
	public Contract getInvoiceLectureConnection() throws Exception {
		if(contract != null) {
			return contract;
		} else {
			this.createConnectionInvoceContract();
			
			if(contract != null) {
				return contract;
			} else {
				System.out.println("Unable to connect to ledger");
				return null;
			}
		}
	}

	// helper function for getting connected to the gateway
	private Gateway connect() throws Exception {
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

}
