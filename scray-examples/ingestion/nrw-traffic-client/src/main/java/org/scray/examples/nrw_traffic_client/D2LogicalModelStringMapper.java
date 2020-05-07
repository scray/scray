package org.scray.examples.nrw_traffic_client;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.scray.examples.traffic.data.D2LogicalModel;
import org.scray.examples.traffic.data.D2LogicalModel.PayloadPublication;
import org.scray.examples.traffic.data.D2LogicalModel.PayloadPublication.ElaboratedData;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.JacksonXmlModule;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class D2LogicalModelStringMapper {
    
    final XmlMapper objectMapper;
    final ObjectMapper mapper;

    public D2LogicalModelStringMapper() {
        mapper = new ObjectMapper();
        JacksonXmlModule module = new JacksonXmlModule();
        
        module.setDefaultUseWrapper(false);
        objectMapper = new XmlMapper(module);
        
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
    }

    public List<String> convertXmlToJson(InputStream xml) {
        List<String> elaboratedData = new ArrayList<String>();
        
        try {
            D2LogicalModel p = objectMapper.readValue(xml, D2LogicalModel.class);
            
            elaboratedData.add(mapper.writeValueAsString(p));

        } catch (JsonParseException e) {
            e.printStackTrace();
            elaboratedData.add(this.createErrorObject(e));
        } catch (JsonMappingException e) {
            e.printStackTrace();
            elaboratedData.add(this.createErrorObject(e));
        } catch (IOException e) {
            e.printStackTrace();
            elaboratedData.add(this.createErrorObject(e));
        }
        
        return elaboratedData;
    }
    
    public String createErrorObject(Exception e) {
        return "{" + "\"error\": \"" + e.getLocalizedMessage() + "\"}";
    }
}
