package org.scray.projects.hyperledger_fabric.tools.wallet_creator;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.PrivateKey;

import org.hyperledger.fabric.gateway.Identities;
import org.hyperledger.fabric.gateway.Identity;
import org.hyperledger.fabric.gateway.Wallet;
import org.hyperledger.fabric.gateway.Wallets;
import org.hyperledger.fabric.sdk.identity.X509Enrollment;

public class WalletCreator {

	public static void main(String[] args) throws Exception {
		String username = args[2];
		
		// Create a wallet for managing identities
		Wallet wallet = Wallets.newFileSystemWallet(Paths.get("wallet"));

		// Enroll the admin user, and import the new identity into the wallet.
		String keyPath =  args[0]; //"/home/stefan/git/scray/projects/invoice-hyperledger-fabric/scripts/wallet-creator/target/Admin@org1.example.com/key.pem";
		String key = new String(Files.readAllBytes((new File(keyPath)).toPath()), Charset.defaultCharset());
		PrivateKey privKey = Identities.readPrivateKey(key); //PEM private key
		
		
		String certPath = args[1]; //"/home/stefan/git/scray/projects/invoice-hyperledger-fabric/scripts/wallet-creator/target/Admin@org1.example.com/user.crt";
		String cert = new String(Files.readAllBytes((new File(certPath)).toPath()), Charset.defaultCharset());
		
		X509Enrollment enrollement = new X509Enrollment(privKey, cert);
		
		Identity user = Identities.newX509Identity("Org1MSP", enrollement);
		wallet.put(username, user);
		System.out.println("Successfully enrolled user \"" + username + "\" and imported it into the wallet");
	}
}
