package org.scray.projects.hyperledger_fabric.invoice_service.api.impl;

import org.scray.projects.hyperledger_fabric.invoice_service.HFabricConnection;
import org.scray.projects.hyperledger_fabric.invoice_service.api.*;
import org.scray.projects.hyperledger_fabric.invoice_service.model.*;

import org.scray.projects.hyperledger_fabric.invoice_service.model.Invoice;

import java.util.List;
import java.util.concurrent.TimeoutException;

import org.scray.projects.hyperledger_fabric.invoice_service.api.NotFoundException;
import org.scray.projects.hyperledger_fabric.invoice_service.mapper.HFabricMapper;

import java.io.InputStream;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.hyperledger.fabric.gateway.Contract;
import org.hyperledger.fabric.gateway.ContractException;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;

@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaJerseyServerCodegen", date = "2020-09-07T22:23:20.829+02:00[Europe/Berlin]")
public class InvoiceApiServiceImpl extends InvoiceApiService {

    HFabricMapper mapper = null;

    private void initMapper() {
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
        String data = "ERROR: ";
        try {
            if(mapper == null) {
                initMapper();
            }
            data = new String(mapper.getInvoice(invoiceId, securityContext));
        } catch (ContractException e) {
        	data +=  e.getLocalizedMessage();
            e.printStackTrace();
            return Response.status(400).entity(new ApiResponseMessage(ApiResponseMessage.ERROR, data)).build();
        }
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, data)).build();
    }

    @Override
    public Response sendInvoice(String invoiceId, Invoice invoiceItem, SecurityContext securityContext)
            throws NotFoundException {
        try {
            
            if(mapper == null) {
                initMapper();
            }
            
            mapper.addInvocie(invoiceId, invoiceItem);
        } catch (ContractException | TimeoutException | InterruptedException e) {
        	String data = e.getLocalizedMessage();
            e.printStackTrace();
            return Response.status(400).entity(new ApiResponseMessage(ApiResponseMessage.ERROR, data)).build();
            
        }

        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "")).build();
    }
}
