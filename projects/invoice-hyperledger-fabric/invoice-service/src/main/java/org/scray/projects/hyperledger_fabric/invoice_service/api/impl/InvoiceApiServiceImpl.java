package org.scray.projects.hyperledger_fabric.invoice_service.api.impl;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import org.hyperledger.fabric.gateway.Contract;
import org.hyperledger.fabric.gateway.ContractException;
import org.scray.projects.hyperledger_fabric.invoice_service.BasicConfigParameters;
import org.scray.projects.hyperledger_fabric.invoice_service.ConfigFileLoader;
import org.scray.projects.hyperledger_fabric.invoice_service.HFabricConnection;
import org.scray.projects.hyperledger_fabric.invoice_service.api.ApiResponseMessage;
import org.scray.projects.hyperledger_fabric.invoice_service.api.InvoiceApiService;
import org.scray.projects.hyperledger_fabric.invoice_service.api.NotFoundException;
import org.scray.projects.hyperledger_fabric.invoice_service.mapper.HFabricMapper;
import org.scray.projects.hyperledger_fabric.invoice_service.model.Invoice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaJerseyServerCodegen", date = "2020-09-07T22:23:20.829+02:00[Europe/Berlin]")
public class InvoiceApiServiceImpl extends InvoiceApiService {

    private HFabricMapper mapper = null;
    private BasicConfigParameters parms = null;

    public InvoiceApiServiceImpl(BasicConfigParameters parms) {
    	parms = ConfigFileLoader.readFromFile();
        this.parms = parms;        
    }
    
    private void initMapper() {
        HFabricConnection con = new HFabricConnection(parms);

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
