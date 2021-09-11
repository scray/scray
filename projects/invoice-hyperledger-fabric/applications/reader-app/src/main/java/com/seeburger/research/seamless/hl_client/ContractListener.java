package com.seeburger.research.seamless.hl_client;

import org.hyperledger.fabric.gateway.ContractEvent;

import java.util.function.Consumer;

public class ContractListener implements Consumer<ContractEvent> {

    @Override
    public void accept(ContractEvent contractEvent) {
        System.out.println(contractEvent);
    }
}
