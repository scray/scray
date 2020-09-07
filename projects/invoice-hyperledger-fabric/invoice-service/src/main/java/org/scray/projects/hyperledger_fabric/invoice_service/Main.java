package org.scray.projects.hyperledger_fabric.invoice_service;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

import org.hyperledger.fabric.gateway.Contract;
import org.scray.projects.hyperledger_fabric.invoice_service.mapper.HFabricMapper;
import org.scray.projects.hyperledger_fabric.invoice_service.model.Invoice;

public class Main {
	static {
		System.setProperty("org.hyperledger.fabric.sdk.service_discovery.as_localhost", "false");
	}

	public static void main(String[] args) throws Exception {
		HFabricConnection con = new HFabricConnection();

		con.createConnectionInvoceContract();
		Contract lecture = con.getInvoiceLectureConnection();

		HFabricMapper mapper = new HFabricMapper(lecture);

		Invoice invoice = new Invoice();
		invoice.setId(UUID.randomUUID());
		//invoice.setDate(new OffsetDateTime(null, null));
		invoice.setTotal(new BigDecimal(3));
		invoice.setState("NEW");

		mapper.addInvocie("4711", invoice);
		System.out.println(mapper.getInvoice("4711", null));
	}

}
