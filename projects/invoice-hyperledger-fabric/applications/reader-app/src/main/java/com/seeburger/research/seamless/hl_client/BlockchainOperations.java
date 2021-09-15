/*
 * BlockchainWriter.java
 *
 * created at 2021-07-01 by st.obermeier <YOURMAILADDRESS>
 *
 * Copyright (c) SEEBURGER AG, Germany. All Rights Reserved.
 */
package com.seeburger.research.seamless.hl_client;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

import org.hyperledger.fabric.gateway.Contract;
import org.hyperledger.fabric.gateway.ContractException;
import org.hyperledger.fabric.gateway.Gateway;
import org.hyperledger.fabric.gateway.Network;
import org.hyperledger.fabric.gateway.Wallet;
import org.hyperledger.fabric.gateway.Wallets;

public class BlockchainOperations
{
    String channel = "mychannel";
    String smartContract = "basic";
    String walletPathString = "";
    String userName = "";
    Gateway gateway = null;

    JsonMapper j = null;

    public BlockchainOperations(String channel, String smartContract, String userName, String walletPath)
    {
        super();
        this.channel = channel;
        this.smartContract = smartContract;
        this.walletPathString = walletPath;
        this.j = new JsonMapper();
        this.userName = userName;
    }


    static {
        System.setProperty("org.hyperledger.fabric.sdk.service_discovery.as_localhost", "false");
    }

    public String write(String id) {

        String resultString = "OK";

            try
            {
                if(gateway == null) {
                    gateway = connect(userName);
                }

                // get the network and contract
                Network network = gateway.getNetwork(channel);
                Contract contract = network.getContract(smartContract);

                contract.submitTransaction("CreateAsset",
                        id,
                            "x509::CN=otto\\ ,OU=admin,O=kubernetes.research.dev.seeburger.de\\ ::CN=ca.peer2.kubernetes.research.dev.seeburger.de,O=peer2.kubernetes.research.dev.seeburger.de,L=Bretten,ST=Baden,C=DE"
                        );
            }
            catch (Exception e)
            {
                e.printStackTrace();
                resultString = e.toString();
            }

            return resultString;
    }

    public String read(String methodName) {
        String data = "{}";

            try
            {
                if(gateway == null) {
                    gateway = connect(userName);
                }
                // get the network and contract
                Network network = gateway.getNetwork(channel);
                Contract contract = network.getContract("basic");
                data = new String(contract.evaluateTransaction(methodName));
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }


        return data;
    }



    // helper function for getting connected to the gateway
    public Gateway connect(String userName) throws Exception{
        // Load a file system based wallet for managing identities.
        Path walletPath = Paths.get(walletPathString);


        Wallet wallet = Wallets.newFileSystemWallet(walletPath);
        // load a CCP
//      Path networkConfigPath = Paths.get("C:\\Users\\st.obermeier\\git\\fabric-samples\\test-network\\organizations\\peerOrganizations\\org1.example.com\\connection-org1.yaml");

        Path networkConfigPath = Paths.get(walletPathString + File.separator + "connection-org1.yaml");
        System.out.println(networkConfigPath);
        Gateway.Builder builder = Gateway.createBuilder();
        System.out.println("Wallet path: " + walletPathString + "\t" + wallet.list());
        builder.identity(wallet, userName).networkConfig(networkConfigPath).discovery(true);
        return builder.connect();
    }
}



