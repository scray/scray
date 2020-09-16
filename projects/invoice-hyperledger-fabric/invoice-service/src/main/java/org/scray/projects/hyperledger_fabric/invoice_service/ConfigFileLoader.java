package org.scray.projects.hyperledger_fabric.invoice_service;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class ConfigFileLoader {
    
    public static BasicConfigParameters readFromFile() {
        BasicConfigParameters params = null;
        
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
        try {
           params = mapper.readValue(new File("config/config.yaml"), BasicConfigParameters.class);
           
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return params;
    }

}
