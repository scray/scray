package org.openapitools.api.factories;

import org.openapitools.api.InvoiceApiService;
import org.openapitools.api.impl.InvoiceApiServiceImpl;

@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaJerseyServerCodegen", date = "2020-09-04T11:07:59.029+02:00[Europe/Berlin]")
public class InvoiceApiServiceFactory {
    private final static InvoiceApiService service = new InvoiceApiServiceImpl();

    public static InvoiceApiService getInvoiceApi() {
        return service;
    }
}
