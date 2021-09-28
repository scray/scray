package org.scray.hyperledger.fabric.example.app;

import org.hyperledger.fabric.gateway.ContractEvent;

import java.util.function.Consumer;

public class ContractListener implements Consumer<ContractEvent> {

    @Override
    public void accept(ContractEvent contractEvent) {
        System.out.println(contractEvent);
    }
}
