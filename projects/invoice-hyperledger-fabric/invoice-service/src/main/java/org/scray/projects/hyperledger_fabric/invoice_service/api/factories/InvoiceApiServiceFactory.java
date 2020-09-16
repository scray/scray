package org.scray.projects.hyperledger_fabric.invoice_service.api.factories;

import org.scray.projects.hyperledger_fabric.invoice_service.BasicConfigParameters;
import org.scray.projects.hyperledger_fabric.invoice_service.api.InvoiceApiService;
import org.scray.projects.hyperledger_fabric.invoice_service.api.impl.InvoiceApiServiceImpl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaJerseyServerCodegen", date = "2020-09-07T22:23:20.829+02:00[Europe/Berlin]")
public class InvoiceApiServiceFactory {
    

    
    private final static InvoiceApiService service = new InvoiceApiServiceImpl(null); 

    public static InvoiceApiService getInvoiceApi() {
        
        
        
        
        return service;
    }
}
