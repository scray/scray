package org.openapitools.api.impl;

import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import org.hyperledger.fabric.gateway.Contract;
import org.hyperledger.fabric.gateway.ContractException;
import org.openapitools.api.ApiResponseMessage;
import org.openapitools.api.InvoiceApiService;
import org.openapitools.api.NotFoundException;
import org.openapitools.model.Invoice;
import org.scray.projects.hyperledger_fabric.invoice_service.HFabricConnection;
import org.scray.projects.hyperledger_fabric.invoice_service.mapper.HFabricMapper;

@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaJerseyServerCodegen", date = "2020-09-04T11:07:59.029+02:00[Europe/Berlin]")
public class InvoiceApiServiceImpl extends InvoiceApiService {
	HFabricMapper mapper = null;

	public InvoiceApiServiceImpl() {
		HFabricConnection con = new HFabricConnection();

		try {
			con.createConnectionInvoceContract();
			Contract lecture = con.getInvoiceLectureConnection();

			this.mapper = new HFabricMapper(lecture);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public Response getInvoice(String invoiceId, SecurityContext securityContext) throws NotFoundException {
		String data = "ERROR";
		try {
			data = new String(mapper.getInvoice(invoiceId, securityContext));
		} catch (ContractException e) {
			e.printStackTrace();
		}
		return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, data)).build();
	}

	@Override
	public Response sendInvoice(String invoiceId, Invoice invoiceItem, SecurityContext securityContext)
			throws NotFoundException {
		try {
			mapper.addInvocie(invoiceId, invoiceItem);
		} catch (ContractException | TimeoutException | InterruptedException e) {
			e.printStackTrace();
		}
		
		return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
	}
}
