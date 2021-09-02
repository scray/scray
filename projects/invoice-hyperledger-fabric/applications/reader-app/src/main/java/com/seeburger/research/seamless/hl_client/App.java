/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

// Running TestApp:
// gradle runApp

package com.seeburger.research.seamless.hl_client;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.hyperledger.fabric.gateway.Contract;
import org.hyperledger.fabric.gateway.Gateway;
import org.hyperledger.fabric.gateway.Network;
import org.hyperledger.fabric.gateway.Wallet;
import org.hyperledger.fabric.gateway.Wallets;

public class App {

	public static void main(String[] args) throws Exception {
	    BlockchainOperations op = new BlockchainOperations("c1", "basic", "otto", "C:\\Users\\st.obermeier\\git\\scray\\projects\\invoice-hyperledger-fabric\\applications\\reader-app\\wallet");

	    op.write("order1" + Math.random());

	    String assets = op.read("GetAllAssets");
	    System.out.println(assets);
	}


}
