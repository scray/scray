/*
 * BlockchainWriter.java
 *
 * created at 2021-07-01 by st.obermeier <YOURMAILADDRESS>
 *
 * Copyright (c) SEEBURGER AG, Germany. All Rights Reserved.
 */
package org.scray.hyperledger.fabric.example.app;

import org.hyperledger.fabric.gateway.*;
import org.hyperledger.fabric.sdk.Orderer;
import org.hyperledger.fabric.sdk.Peer;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

public class ListenOnContractEvents
{
    String channel = "mychannel";
    String smartContract = "basic";
    String walletPathString = "";
    String userName = "";
    Gateway gateway = null;

    JsonMapper j = null;

    public ListenOnContractEvents(String channel, String smartContract, String userName, String walletPath)
    {
        super();
        this.channel = channel;
        this.smartContract = smartContract;
        this.walletPathString = walletPath;
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
                        "2",
                        "3",
                        "4",
                        "5",
                        "6",
                        "true",
                        "true",
                        "true",
                        "true",
                        "true",
                        "true",
                        "true",
                        "true",
                        "true");
            }
            catch (Exception e)
            {
                e.printStackTrace();
                resultString = e.toString();
            }

            return resultString;
    }

    public String showChannelInfos(String methodName) {
        String data = "{}";

            try
            {
                if(gateway == null) {
                    gateway = connect(userName);
                }
                // get the network and contract
                Network network = gateway.getNetwork(channel);
                Contract contract = network.getContract("basic");

                Collection<String> installedChaincode = network.getChannel().getDiscoveredChaincodeNames();
                Collection<Orderer> orderer = network.getChannel().getOrderers();
                Collection<Peer> peers = network.getChannel().getPeers();

                System.out.println("Installed chaincode: " + installedChaincode);
                System.out.println("Orderer list: " + orderer);
                System.out.println("Peers " + peers);

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
//      Path networkConfigPath = Paths.get("C:\\Users\\st.obermeier\\git\\fabric-samples\\test-network\\organizations\\peerOrganizations\\org1.example.com\\connection.yaml");

        Path networkConfigPath = Paths.get(walletPathString + File.separator + "connection.yaml");
        System.out.println(networkConfigPath);
        Gateway.Builder builder = Gateway.createBuilder();
        System.out.println("Wallet path: " + walletPathString + "\t" + wallet.list());
        builder.identity(wallet, userName).networkConfig(networkConfigPath).discovery(true);
        return builder.connect();
    }
}



