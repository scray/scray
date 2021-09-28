package org.scray.hyperledger.fabric.example.app;

public class ListenerMain {

    public static void main(String[] args) throws InterruptedException {
        ListenOnContractEvents op = new ListenOnContractEvents("c6", "basic", "otto", "C:\\Users\\st.obermeier\\git\\scray\\projects\\invoice-hyperledger-fabric\\applications\\reader-app\\wallet");

        String assets = op.showChannelInfos("GetAllAssets");
        System.out.println(assets);
    }
}
