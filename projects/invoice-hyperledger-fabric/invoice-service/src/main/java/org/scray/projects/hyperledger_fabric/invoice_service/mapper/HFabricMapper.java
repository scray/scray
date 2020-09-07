package org.scray.projects.hyperledger_fabric.invoice_service.mapper;

import java.util.concurrent.TimeoutException;

import javax.ws.rs.core.SecurityContext;

import org.hyperledger.fabric.gateway.Contract;
import org.hyperledger.fabric.gateway.ContractException;
import org.scray.projects.hyperledger_fabric.invoice_service.model.Invoice;

public class HFabricMapper {
	
	static {
		System.setProperty("org.hyperledger.fabric.sdk.service_discovery.as_localhost", "false");
	}
	
	Contract contract = null;
	
	public HFabricMapper(Contract contract) {
		this.contract = contract;
	}
	
	public void addInvocie(String invoiceId, Invoice invoiceItem) throws ContractException, TimeoutException, InterruptedException {
		System.out.println(contract);
		System.out.println(invoiceItem);
		
		
		contract.submitTransaction("InitLedger");
		
		contract.submitTransaction("CreateAsset", invoiceId, invoiceItem.getDate().toString(), invoiceItem.getState(), invoiceItem.getTotal().toString(), "453");
	}
	
	public String getInvoice(String invoiceId, SecurityContext securityContext) throws ContractException {
		return new String(contract.evaluateTransaction("ReadAsset", invoiceId));
	}

}
